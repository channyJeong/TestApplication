version: '2'
services:
  zookeeper:
    image: wurstmeister/zookeeper
    ports:
      - "2181:2181"
    restart: unless-stopped
    networks:
      project_net2:
        ipv4_address: 172.22.0.3

  kafka:
    build: .
    ports:
      - "9092:9092"
    environment:
      DOCKER_API_VERSION: 1.22
      KAFKA_ADVERTISED_HOST_NAME: localhost
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://localhost:9092'
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_MESSAGE_MAX_BYTES: 10000000
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_DELETE_TOPIC_ENABLE: 'true'
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - /Users/a10150389/docker/data/kafka:/kafka
    restart: unless-stopped
    networks:
      project_net2:
        ipv4_address: 172.22.0.5

networks:
  project_net2:
    ipam:
      driver: default
      config:
        - subnet: 172.22.0.0/16
