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
#define BUFFER_SIZE 2048

typedef struct {
    int socket_fd;
    char username[64];
    char current_room[64];
    int registered;
    char rx_buffer[BUFFER_SIZE];
    int rx_len;
} client_t;

typedef struct {
    char name[64];
    int active;
} room_t;

client_t clients[MAX_CLIENTS];
room_t rooms[MAX_ROOMS];
pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;

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

void remove_client(int socket_fd) {
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket_fd == socket_fd) {
            if (clients[i].registered) {
                char log_buf[128];
                snprintf(log_buf, sizeof(log_buf), "User disconnected: %s", clients[i].username);
                log_event(log_buf);
            }
            close(clients[i].socket_fd);
            memset(&clients[i], 0, sizeof(client_t));
            break;
        }
    }
    pthread_mutex_unlock(&global_mutex);
}

void process_command(int client_idx, char *line) {
    int fd = clients[client_idx].socket_fd;
    
    // 1. REGISTER Command
    if (strncmp(line, "REGISTER ", 9) == 0) {
        char uname[64];
        if (sscanf(line + 9, "%s", uname) != 1) {
            send_response(fd, "ERR 000 INVALID_COMMAND");
            return;
        }
        
        pthread_mutex_lock(&global_mutex);
        int taken = 0;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].registered && strcmp(clients[i].username, uname) == 0) {
                taken = 1; 
                break;
            }
        }
        if (taken) {
            pthread_mutex_unlock(&global_mutex);
            send_response(fd, "ERR 001 USERNAME_TAKEN");
        } else {
            strcpy(clients[client_idx].username, uname);
            clients[client_idx].registered = 1;
            pthread_mutex_unlock(&global_mutex);
            
            send_response(fd, "OK REGISTERED");
            char log_buf[128];
            snprintf(log_buf, sizeof(log_buf), "User registered: %s", uname);
            log_event(log_buf);
        }
        return;
    }

    // Unregistered users check
    if (!clients[client_idx].registered) {
        send_response(fd, "ERR 002 NOT_REGISTERED");
        return;
    }

    // 2. LIST Command
    if (strcmp(line, "LIST") == 0) {
        char list_buf[BUFFER_SIZE] = "OK USERS ";
        pthread_mutex_lock(&global_mutex);
        int first = 1;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].registered) {
                if (!first) strcat(list_buf, ",");
                strcat(list_buf, clients[i].username);
                first = 0;
            }
        }
        pthread_mutex_unlock(&global_mutex);
        send_response(fd, list_buf);
    }
    // 3. BCAST Command
    else if (strncmp(line, "BCAST ", 6) == 0) {
        send_response(fd, "OK SENT");
        char msg_buf[BUFFER_SIZE];
        snprintf(msg_buf, sizeof(msg_buf), "MSG BCAST %s %s %s\n", clients[client_idx].username, line + 6, NID_TAG);
        
        pthread_mutex_lock(&global_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].registered && clients[i].socket_fd != fd) {
                send(clients[i].socket_fd, msg_buf, strlen(msg_buf), 0);
            }
        }
        pthread_mutex_unlock(&global_mutex);
    }
    // 4. PMSG Command
    else if (strncmp(line, "PMSG ", 5) == 0) {
        char target_user[64], msg[BUFFER_SIZE];
        if (sscanf(line + 5, "%s %[^\n]", target_user, msg) < 2) {
            send_response(fd, "ERR 000 INVALID_COMMAND");
            return;
        }

        pthread_mutex_lock(&global_mutex);
        int target_fd = -1;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].registered && strcmp(clients[i].username, target_user) == 0) {
                target_fd = clients[i].socket_fd;
                break;
            }
        }
        pthread_mutex_unlock(&global_mutex);

        if (target_fd != -1) {
            send_response(fd, "OK SENT");
            char pmsg_buf[BUFFER_SIZE];
            snprintf(pmsg_buf, sizeof(pmsg_buf), "MSG PMSG %s %s %s\n", clients[client_idx].username, msg, NID_TAG);
            send(target_fd, pmsg_buf, strlen(pmsg_buf), 0);
        } else {
            send_response(fd, "ERR 003 USER_NOT_FOUND");
        }
    }
    // 5. Room Commands: JOIN, LEAVE, ROOMS, RMSG
    else if (strncmp(line, "JOIN ", 5) == 0) {
        char room_name[64];
        sscanf(line + 5, "%s", room_name);
        
        pthread_mutex_lock(&global_mutex);
        strcpy(clients[client_idx].current_room, room_name);
        
        int exists = 0;
        for(int i=0; i<MAX_ROOMS; i++) {
            if(rooms[i].active && strcmp(rooms[i].name, room_name) == 0) { exists = 1; break; }
        }
        if(!exists) {
            for(int i=0; i<MAX_ROOMS; i++) {
                if(!rooms[i].active) {
                    strcpy(rooms[i].name, room_name);
                    rooms[i].active = 1;
                    break;
                }
            }
        }
        pthread_mutex_unlock(&global_mutex);
        send_response(fd, "OK JOINED");
    }
    else if (strcmp(line, "LEAVE") == 0) {
        pthread_mutex_lock(&global_mutex);
        clients[client_idx].current_room[0] = '\0';
        pthread_mutex_unlock(&global_mutex);
        send_response(fd, "OK LEFT");
    }
    else if (strcmp(line, "ROOMS") == 0) {
        char rooms_buf[BUFFER_SIZE] = "OK ROOMS ";
        pthread_mutex_lock(&global_mutex);
        int first = 1;
        for (int i = 0; i < MAX_ROOMS; i++) {
            if (rooms[i].active) {
                if (!first) strcat(rooms_buf, ",");
                strcat(rooms_buf, rooms[i].name);
                first = 0;
            }
        }
        pthread_mutex_unlock(&global_mutex);
        send_response(fd, rooms_buf);
    }
    else if (strncmp(line, "RMSG ", 5) == 0) {
        char room_name[64], msg[BUFFER_SIZE];
        if (sscanf(line + 5, "%s %[^\n]", room_name, msg) < 2) {
            send_response(fd, "ERR 000 INVALID_COMMAND");
            return;
        }
        
        send_response(fd, "OK SENT");
        char rmsg_buf[BUFFER_SIZE];
        snprintf(rmsg_buf, sizeof(rmsg_buf), "MSG RMSG %s %s %s %s\n", room_name, clients[client_idx].username, msg, NID_TAG);
        
        pthread_mutex_lock(&global_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].registered && strcmp(clients[i].current_room, room_name) == 0 && clients[i].socket_fd != fd) {
                send(clients[i].socket_fd, rmsg_buf, strlen(rmsg_buf), 0);
            }
        }
        pthread_mutex_unlock(&global_mutex);
    }
    // 6. SENDFILE Command (Personalised Directory Support)
    else if (strncmp(line, "SENDFILE ", 9) == 0) {
        char target_user[64], filename[128], content[BUFFER_SIZE];
        if (sscanf(line + 9, "%s %s %[^\n]", target_user, filename, content) < 3) {
            send_response(fd, "ERR 000 INVALID_COMMAND");
            return;
        }

        // Create target directory if not exists
        char user_dir[256];
        snprintf(user_dir, sizeof(user_dir), "%s/%s", STORAGE_DIR, target_user);
        mkdir("./storage", 0777);
        mkdir(STORAGE_DIR, 0777);
        mkdir(user_dir, 0777);

        char filepath[512];
        snprintf(filepath, sizeof(filepath), "%s/%s", user_dir, filename);
        
        FILE *fp = fopen(filepath, "w");
        if (fp) {
            fputs(content, fp);
            fclose(fp);
            send_response(fd, "OK FILE_RECEIVED");
        } else {
            send_response(fd, "ERR 004 FILE_SAVE_FAILED");
        }
    }
    // 7. QUIT Command
    else if (strcmp(line, "QUIT") == 0) {
        send_response(fd, "OK BYE");
    }
    else {
        send_response(fd, "ERR 000 INVALID_COMMAND");
    }
}

void *handle_client(void *arg) {
    int fd = *(int *)arg;
    free(arg);

    int client_idx = -1;
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket_fd == fd) {
            client_idx = i;
            break;
        }
    }
    pthread_mutex_unlock(&global_mutex);

    if (client_idx == -1) return NULL;

    char temp_buf[256];
    while (1) {
        int bytes = recv(fd, temp_buf, sizeof(temp_buf) - 1, 0);
        if (bytes <= 0) break;
        temp_buf[bytes] = '\0';

        pthread_mutex_lock(&global_mutex);
        strcat(clients[client_idx].rx_buffer, temp_buf);
        clients[client_idx].rx_len += bytes;

        // Framing Logic (\n வர வர பிரித்தல்)
        char *line_end;
        while ((line_end = strchr(clients[client_idx].rx_buffer, '\n')) != NULL) {
            *line_end = '\0';
            
            // Remove \r if exists
            if (line_end > clients[client_idx].rx_buffer && *(line_end - 1) == '\r') {
                *(line_end - 1) = '\0';
            }

            char single_cmd[BUFFER_SIZE];
            strcpy(single_cmd, clients[client_idx].rx_buffer);

            int processed_len = line_end - clients[client_idx].rx_buffer + 1;
            memmove(clients[client_idx].rx_buffer, line_end + 1, clients[client_idx].rx_len - processed_len + 1);
            clients[client_idx].rx_len -= processed_len;

            pthread_mutex_unlock(&global_mutex);
            
            process_command(client_idx, single_cmd);
            if (strcmp(single_cmd, "QUIT") == 0) {
                remove_client(fd);
                return NULL;
            }

            pthread_mutex_lock(&global_mutex);
        }
        pthread_mutex_unlock(&global_mutex);
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

        pthread_mutex_lock(&global_mutex);
        int added = 0;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].socket_fd == 0) {
                clients[i].socket_fd = *new_sock;
                added = 1;
                break;
            }
        }
        pthread_mutex_unlock(&global_mutex);

        if (added) {
            pthread_t thread;
            pthread_create(&thread, NULL, handle_client, new_sock);
            pthread_detach(thread);
        } else {
            close(*new_sock);
            free(new_sock);
        }
    }

    return 0;
}
