# 使用官方 Airflow 基礎映像
FROM apache/airflow:2.10.2

# 切換到 root 以安裝依賴項
USER root

# 安裝必要的依賴項
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    libsqlite3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 下載並安裝 Python 3.9.20
RUN wget https://www.python.org/ftp/python/3.9.20/Python-3.9.20.tgz \
    && tar -xf Python-3.9.20.tgz \
    && cd Python-3.9.20 \
    && ./configure --enable-optimizations \
    && make -j $(nproc) \
    && make altinstall \
    && cd .. \
    && rm -rf Python-3.9.20.tgz Python-3.9.20

# 更新 Python 版本
RUN update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.9 1
RUN update-alternatives --install /usr/bin/python python /usr/local/bin/python3.9 1

# 安裝 pip
RUN curl https://bootstrap.pypa.io/get-pip.py | python3.9

# 切換回 airflow 使用者
USER airflow

# 安裝 Python 套件
COPY requirements.txt /requirements.txt
RUN python3.9 -m pip install --upgrade pip \
    && python3.9 -m pip install -r /requirements.txt

# 設置 Airflow 用於 CeleryExecutor 的環境變量
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor
ENV AIRFLOW__CELERY__BROKER_URL=redis://10.140.0.7:6379/0
ENV AIRFLOW__CELERY__RESULT_BACKEND=redis://10.140.0.7:6379/0
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=mysql+mysqlconnector://airflow:airflow@10.140.0.7:3306/airflow

# 啟動 Airflow Worker
CMD ["airflow", "celery", "worker"]