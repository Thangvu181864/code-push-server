# Stage 1: Build the application
FROM node:18-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy package files first to leverage Docker cache
COPY package*.json ./

# Install all dependencies (including devDependencies for building)
RUN npm install

# Copy the rest of the application source code
COPY . .

# Compile the TypeScript/Server code
RUN npm run build

# Stage 2: Production environment
FROM node:18-alpine AS runner

WORKDIR /app

# Set production environment
ENV NODE_ENV=production

# Copy only the built artifacts and production dependencies from the builder
COPY --from=builder /app/bin ./bin
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/node_modules ./node_modules
# Note: In a strict prod setup, you'd run 'npm prune --production' in Stage 1

# Expose the default port (adjust if you changed it in .env)
EXPOSE 3000

# The command to launch the server
CMD ["npm", "run", "start:env"]