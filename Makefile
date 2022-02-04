test:
	cargo test

itest:
	source refresh-token.sh
	cargo test -- --ignored

clean:
	cargo clean

