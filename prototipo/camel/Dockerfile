FROM eclipse-temurin:17-jdk

RUN apt-get update && apt-get install -y --no-install-recommends curl unzip \
    && curl -Ls https://sh.jbang.dev | bash -s - app setup \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.jbang/bin:${PATH}"

RUN jbang trust add https://github.com/apache/
WORKDIR /app
COPY routes.yaml application.properties ./

CMD ["jbang", "-Dcamel.jbang.version=4.4.0", "camel@apache/camel", "run", "routes.yaml", "--properties=application.properties"]
