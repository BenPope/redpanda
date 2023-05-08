#!/bin/bash

clang -target wasm32 -iquote . \
  -Wl,--no-entry -nostdlib -std=c++20 -mbulk-memory \
 -ffunction-sections -fdata-sections -fvisibility=hidden \
  -o identity_transform.wasm cc/identity_transform.cc 

clang -target wasm32 -iquote . \
  -Wl,--no-entry -nostdlib -std=c++20 -mbulk-memory \
 -ffunction-sections -fdata-sections -fvisibility=hidden \
  -o bang_transform.wasm cc/bang_transform.cc 

clang -target wasm32 -iquote . \
  -Wl,--no-entry -nostdlib -std=c++20 -mbulk-memory \
 -ffunction-sections -fdata-sections -fvisibility=hidden \
 -DOPENAI_API_KEY="\"$OPENAI_API_KEY\"" \
  -o ai_transform.wasm cc/ai_transform.cc 

tinygo build -target wasi \
  -panic trap -scheduler none -gc conservative \
  -o golang_identity_transform.wasm identity_transform.go
