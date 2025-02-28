#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <sys/wait.h>

int pid = -1;

void handle_signal(int sig)
{
    kill(pid, SIGINT);
}

int main(int argc, char **argv)
{
    int status, ret;
    char **myargs;
    char *myenv[] = {NULL};

    if (argc < 2)
    {
        printf("usage: sv <program> <args...>\n");
        exit(2);
    }

    if ((pid = fork()) < 0)
    {
        perror("fork");
        exit(1);
    }

    if (pid == 0)
    {
        // the child
        myargs = argv + 1;
        if (execvp(argv[1], myargs) == -1)
        {
            perror("exec");
            printf("exec error\n");
        }
    }
    else
    {
        // the parent
        signal(SIGTERM, handle_signal);
        signal(SIGINT, handle_signal);

        if ((ret = waitpid(pid, &status, 0)) == -1)
            perror("waitpid");

        if (ret == pid)
        {
            if WIFEXITED (status)
                return WEXITSTATUS(status);
        }
    }
}
