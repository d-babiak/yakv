#include <arpa/inet.h>
#include <assert.h>
#include <stdio.h> 
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define BACK_LOG 5

// will an OS really split 4 bytes into different segments?
// lol wat
// how can you verify that the first four bytes
// are a big-endian u32 encoding the length of the following
// string?
int read_uint32(int client_fd) {
  uint32_t netlong;
  ssize_t n = read(client_fd, &netlong, sizeof netlong);
  assert(n == sizeof netlong);
  return ntohl(netlong);
}

char *read_str(int client_fd, char *buf, size_t len) {
  ssize_t n = read(client_fd, buf, len);
  assert(n == len);
  // also need to append a null byte?
  buf[n] = '\0';
  return buf;
}

int server(int port) {
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  assert(listen_fd >= 0);

  struct sockaddr_in serv_addr = {
    .sin_family = AF_INET,
    .sin_port = htons(port),
    .sin_addr = { .s_addr = INADDR_ANY }
  };

  int err = bind(
    listen_fd, 
    (struct sockaddr *) &serv_addr,
    sizeof serv_addr
  );
  assert(!err);

  err = listen(listen_fd, BACK_LOG);
  assert(!err);

  return listen_fd;
}

int accept_client(int listen_fd) {
  // do we care about this thing?
  struct sockaddr_in client_addr = {
    .sin_family = AF_INET
  };
  socklen_t client_len = sizeof client_addr;

  int client_fd = accept(
    listen_fd,
    (struct sockaddr *) &client_addr,
    &client_len
  );
  assert(client_fd >= 0);

  return client_fd;
}

void handle_request(int client_fd) {
  uint32_t n = read_uint32(client_fd);
  char buf[n];
  char *s = read_str(client_fd, buf, n);
  printf("  Client: %s\n", s);
}

int main(int argc, char *argv[]) {
  assert(argc == 2);
  int port = atoi(argv[1]);

  int listen_fd = server(port);

  int client_fd = accept_client(listen_fd);

  for (;;) {
    handle_request(client_fd);
  }

  close(listen_fd);
  close(client_fd);
}
