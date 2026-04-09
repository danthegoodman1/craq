FROM golang:1.26

RUN curl -fsSL https://opencode.ai/install | bash

ENV PATH="/root/.opencode/bin:${PATH}"

WORKDIR /workspace

COPY . .

EXPOSE 4096

CMD ["opencode", "web", "--port", "4096", "--hostname", "0.0.0.0"]
