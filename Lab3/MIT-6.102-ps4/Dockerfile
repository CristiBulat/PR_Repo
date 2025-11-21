# Use Node.js 22.12 (matching the required version)
FROM node:22.12-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source code and other files
COPY tsconfig.json ./
COPY eslint.config.mjs ./
COPY .npmrc ./
COPY src/ ./src/
COPY test/ ./test/
COPY boards/ ./boards/
COPY public/ ./public/

# Compile TypeScript
RUN npm run compile

# Expose port 8080
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD node -e "require('http').get('http://localhost:8080', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

# Default command
CMD ["npm", "start", "8080", "boards/perfect.txt"]