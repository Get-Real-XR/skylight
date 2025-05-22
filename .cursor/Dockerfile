FROM rust:latest

# Set working directory
WORKDIR /app

# Install common dependencies for development
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*


# Default command
CMD ["bash"]
