FROM node:16 as builder
WORKDIR /usr/src/app
COPY package.json ./
RUN yarn
COPY . ./
RUN yarn build

FROM bitnami/nginx:latest
COPY --from=builder /usr/src/app/dist /dist
EXPOSE 8080
