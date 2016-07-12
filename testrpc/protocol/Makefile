AVDLC=~/node_modules/.bin/avdlc
simple.go: simple.avdl
	$(AVDLC) -b -l go -o . simple.avdl
clean:
	rm -f go/simple.go
