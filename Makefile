CFLAGS+=-fPIC

all: attributes.so

attributes.so: attributes.o
	gcc -shared -o $@ $^

test:
	prove t

clean:
	rm -f *.o *.so
