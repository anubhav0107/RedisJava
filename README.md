# RedisJava: A Redis Clone

Welcome to my Redis clone project!

This project is part of the **Build Your Own Redis** challenge. Redis is an in-memory data structure store often used as a database, cache, message broker, and streaming engine. In this project, I am building a Redis server capable of handling basic commands, reading RDB files, and more. This Redis clone can be used for in-memory caching and other purposes where high-speed data access is required.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Usage](#usage)

## Introduction

Redis is renowned for its simplicity and performance. By building this Redis clone, I aim to understand the inner workings of Redis and the key concepts that contribute to its efficiency. This project covers:

- TCP servers and client connections
- The Redis Protocol (RESP) for client-server communication
- Data persistence using RDB files
- Implementation of basic Redis commands

## Features

- Support for basic Redis commands (GET, SET, DEL, etc.)
- TCP server for handling client connections
- RDB file reading for data persistence
- Modular and extensible codebase

## Usage

Once the server is running, you can connect to it using any Redis client (e.g., redis-cli). Here are some basic commands you can use:

- SET key value: Store a key-value pair
- GET key: Retrieve the value associated with a key
- DEL key: Delete a key-value pair
