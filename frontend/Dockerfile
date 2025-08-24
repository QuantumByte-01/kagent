FROM node:22-alpine AS build
WORKDIR /app

# Set the environment to production inside the container
ENV NODE_ENV=production

# Install only production dependencies
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Copy source code and build the frontend
COPY . ./
RUN npm run build

# Serve the built assets with nginx
FROM nginx:alpine

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
