#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>

#define PORT 7760
#define NID_TAG "NID:6517"
#define LOG_FILE "netmsg_IT23651760.log"
#define STORAGE_DIR "./storage/IT23651760"
#define MAX_CLIENTS 50
#define MAX_ROOMS 20
#define BUFFER_SIZE 1024

typedef struct {
    int socket_fd;
    char username[64];
    int registered;
} client_t;

typedef struct {
    char name[64];
    char members[MAX_CLIENTS][64];
    int member_count;
} room_t;

client_t clients[MAX_CLIENTS];
room_t rooms[MAX_ROOMS];
int room_count = 0;
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

void log_event(const char *event) {
    FILE *f = fopen(LOG_FILE, "a");
    if (!f) return;
    time_t now = time(NULL);
    char *time_str = ctime(&now);
    time_str[strlen(time_str) - 1] = '\0';
    fprintf(f, "[%s] %s\n", time_str, event);
    fclose(f);
}

void send_response(int socket_fd, const char *msg) {
    char response[BUFFER_SIZE];
    snprintf(response, sizeof(response), "%s %s\n", msg, NID_TAG);
    send(socket_fd, response, strlen(response), 0);
}

void broadcast_message(const char *msg, int sender_fd) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket_fd > 0 && clients[i].registered && clients[i].socket_fd != sender_fd) {
            send(clients[i].socket_fd, msg, strlen(msg), 0);
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

void remove_client(int socket_fd) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket_fd == socket_fd) {
            if (clients[i].registered) {
                char log_buf[128];
                snprintf(log_buf, sizeof(log_buf), "User disconnected: %s", clients[i].username);
                log_event(log_buf);

                char bcast_buf[128];
                snprintf(bcast_buf, sizeof(bcast_buf), "MSG SYSTEM %s left the chat\n", clients[i].username);
                pthread_mutex_unlock(&clients_mutex);
                broadcast_message(bcast_buf, socket_fd);
                pthread_mutex_lock(&clients_mutex);
            }
            close(clients[i].socket_fd);
            clients[i].socket_fd = 0;
            clients[i].registered = 0;
            clients[i].username[0] = '\0';
            break;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

void *handle_client(void *arg) {
    int fd = *(int *)arg;
    free(arg);
    char buffer[BUFFER_SIZE];

    while (1) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes = recv(fd, buffer, BUFFER_SIZE - 1, 0);
        if (bytes <= 0) break;

        buffer[strcspn(buffer, "\r\n")] = 0; // Strip trailing newline

        if (strncmp(buffer, "REGISTER ", 9) == 0) {
            char uname[64];
            sscanf(buffer + 9, "%s", uname);
            
            pthread_mutex_lock(&clients_mutex);
            int taken = 0;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].registered && strcmp(clients[i].username, uname) == 0) {
                    taken = 1; break;
                }
            }
            if (taken) {
                pthread_mutex_unlock(&clients_mutex);
                send_response(fd, "ERR 001 USERNAME_TAKEN");
            } else {
                for (int i = 0; i < MAX_CLIENTS; i++) {
                    if (clients[i].socket_fd == fd) {
                        strcpy(clients[i].username, uname);
                        clients[i].registered = 1;
                        break;
                    }
                }
                pthread_mutex_unlock(&clients_mutex);
                send_response(fd, "OK REGISTERED");
                
                char log_buf[128];
                snprintf(log_buf, sizeof(log_buf), "User registered: %s", uname);
                log_event(log_buf);
            }
        } 
        else if (strcmp(buffer, "LIST") == 0) {
            char list_buf[BUFFER_SIZE] = "OK USERS ";
            pthread_mutex_lock(&clients_mutex);
            int first = 1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].registered) {
                    if (!first) strcat(list_buf, ",");
                    strcat(list_buf, clients[i].username);
                    first = 0;
                }
            }
            pthread_mutex_unlock(&clients_mutex);
            send_response(fd, list_buf);
        }
        else if (strncmp(buffer, "BCAST ", 6) == 0) {
            char sender[64] = "Unknown";
            pthread_mutex_lock(&clients_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].socket_fd == fd) { strcpy(sender, clients[i].username); break; }
            }
            pthread_mutex_unlock(&clients_mutex);

            send_response(fd, "OK SENT");
            char msg_buf[BUFFER_SIZE];
            snprintf(msg_buf, sizeof(msg_buf), "MSG BCAST %s %s\n", sender, buffer + 6);
            broadcast_message(msg_buf, fd);
        }
        else if (strcmp(buffer, "QUIT") == 0) {
            send_response(fd, "OK BYE");
            break;
        }
        else {
            send_response(fd, "ERR 000 INVALID_COMMAND");
        }
    }

    remove_client(fd);
    return NULL;
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));
    listen(server_fd, 10);

    log_event("Server started on port 7760");
    printf("NetMessenger Server (IT23651760) running on port %d...\n", PORT);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int *new_sock = malloc(sizeof(int));
        *new_sock = accept(server_fd, (struct sockaddr *)&client_addr, &addrlen);

        pthread_mutex_lock(&clients_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].socket_fd == 0) {
                clients[i].socket_fd = *new_sock;
                break;
            }
        }
        pthread_mutex_unlock(&clients_mutex);

        pthread_t thread;
        pthread_create(&thread, NULL, handle_client, new_sock);
        pthread_detach(thread);
    }

    return 0;
}
