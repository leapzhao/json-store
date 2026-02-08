#!/bin/bash

# 设置环境
export APP_ENV=${APP_ENV:-local}
export CONFIG_PATH="./config"

# 根据环境加载对应的.env文件
if [ -f ".env.$APP_ENV" ]; then
    export $(cat .env.$APP_ENV | grep -v '^#' | xargs)
fi

# 执行应用
./json-store