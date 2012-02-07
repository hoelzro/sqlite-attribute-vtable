CFLAGS+=-fPIC -Werror

all: attributes.so

attributes.so: attributes.o
	gcc -shared -o $@ $^

test: attributes.so
	prove t

clean:
	rm -f *.o *.so
