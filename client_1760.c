#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

#define PORT 7760
#define BUFFER_SIZE 1024

void *receive_handler(void *socket_desc) {
    int sock = *(int *)socket_desc;
    char buffer[BUFFER_SIZE];
    while (1) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes = recv(sock, buffer, BUFFER_SIZE - 1, 0);
        if (bytes <= 0) {
            printf("\nDisconnected from server.\n");
            exit(0);
        }
        printf("\n[Server Response] %s", buffer);
        printf("NetMessenger> ");
        fflush(stdout);
    }
    return NULL;
}

int main() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("Connection failed!\n");
        return -1;
    }

    printf("Connected to NetMessenger Server (Port 7760)\n");

    pthread_t recv_thread;
    pthread_create(&recv_thread, NULL, receive_handler, &sock);

    char input[BUFFER_SIZE];
    while (1) {
        printf("NetMessenger> ");
        fgets(input, BUFFER_SIZE, stdin);
        send(sock, input, strlen(input), 0);
        if (strncmp(input, "QUIT", 4) == 0) {
            sleep(1);
            break;
        }
    }

    close(sock);
    return 0;
}
