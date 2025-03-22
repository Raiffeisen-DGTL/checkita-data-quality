FROM mcr.microsoft.com/devcontainers/java:1-11-bullseye

# Install minimal Python 3.12 dependencies
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libffi-dev \
    libsqlite3-dev \
    git \
    curl \
    wget \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install pyenv and Python 3.12
USER vscode
ENV PYENV_ROOT=/home/vscode/.pyenv
ENV PATH="${PYENV_ROOT}/bin:${PYENV_ROOT}/shims:${PATH}"
RUN curl https://pyenv.run | bash \
    && ${PYENV_ROOT}/bin/pyenv install 3.12.0 \
    && ${PYENV_ROOT}/bin/pyenv global 3.12.0 \
    && pip install --upgrade pip

# Switch back to root for system installations
USER root

ENV SPARK_VERSION=3.5.1

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf aws awscliv2.zip

# Install Apache Spark
RUN mkdir -p /opt/spark \
    && curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/spark --strip-components=1 \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}

# Switch back to vscode user for development
USER vscode
WORKDIR /home/vscode

# Add environment variables to vscode user's bashrc
RUN echo 'export PYENV_ROOT=/home/vscode/.pyenv' >> ~/.bashrc \
    && echo 'export PATH="${PYENV_ROOT}/bin:${PYENV_ROOT}/shims:${PATH}"' >> ~/.bashrc \
    && echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc \
    && echo 'export PATH="${SPARK_HOME}/bin:${PATH}"' >> ~/.bashrc

CMD ["/bin/bash"]