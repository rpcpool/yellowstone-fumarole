clean: clean-nodejs clean-rust
	rm -rf test-ledger

clean-nodejs:
	rm -rf sdk-clients/examples/typescript/dist
	rm -rf sdk-clients/examples/typescript/node_modules
	rm -rf sdk-clients/yellowstonefumaroleclient-nodejs/dist
	rm -rf sdk-clients/yellowstone-fumarole-client-nodejs/node_modules
	rm -rf sdk-clients/yellowstone-fumarole-client-nodejs/src/encoding
	rm -rf sdk-clients/yellowstone-fumarole-client-nodejs/src/grpc

clean-rust:
	rm -rf target
	rm -rf sdk-clients/yellowstone-fumarole-client-nodejs/solana-encoding-wasm/target

solana-encoding-wasm-clippy:
	cd sdk-clients/yellowstone-fumarole-client-nodejs/solana-encoding-wasm && \
		cargo clippy --target wasm32-unknown-unknown --all-targets

solana-encoding-wasm-build:
	# RUSTFLAGS to disable `mold`
	cd sdk-clients/yellowstone-fumarole-client-nodejs/solana-encoding-wasm && \
		RUSTFLAGS="" cargo build \
			--target wasm32-unknown-unknown \
			--release

	cd sdk-clients/yellowstone-fumarole-client-nodejs/solana-encoding-wasm && \
		rm -rf ../src/encoding/ && \
		wasm-bindgen \
			--target nodejs \
			--out-dir ../src/encoding/ \
			target/wasm32-unknown-unknown/release/yellowstone_grpc_solana_encoding_wasm.wasm && \
		mkdir -p ../dist/encoding/ && \
		cp -ap ../src/encoding/ ../dist/
