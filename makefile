.PHONY: build run

build:
	xcaddy build --with github.com/stepbrobd/certmagic-s3=.

run:
	./caddy run --config ./caddyfile
