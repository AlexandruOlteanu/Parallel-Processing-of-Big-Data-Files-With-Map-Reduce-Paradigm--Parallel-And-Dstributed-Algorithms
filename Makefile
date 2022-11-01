CC = g++
#CFLAGS = -Wall -Wextra -O2
LDLIBS = -lm

build: tema1.o

tema1.o: tema1.cpp
	g++ tema1.cpp -o tema1 -lpthread -Wall -Werror

.PHONY: clean

clean:
	rm -rf *.o tema1