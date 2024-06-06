HOME_DIR="/home/xiezhiyu32768"

# Install Go if not already installed
if ! /usr/local/go/bin/go version &> /dev/null; then
    wget https://golang.org/dl/go1.22.2.linux-amd64.tar.gz
    sudo tar -C /usr/local -xzf go1.22.2.linux-amd64.tar.gz
    echo 'export PATH=$PATH:/usr/local/go/bin' >> ${HOME_DIR}/.profile
    source ${HOME_DIR}/.profile
    if go version; then
        echo "Go has been downloaded successfully"
    else
        echo "Failed to install Go"
        exit 1
    fi
else
    source ${HOME_DIR}/.profile
    echo "Go is already installed"
fi

# Install Redis if not already installed
if ! command -v redis-cli &> /dev/null; then
    sudo apt install -y redis-server
    redis-cli -v
    sudo cp /etc/redis/redis.conf /etc/redis/redis.conf.bak
    sudo sed -i 's/^appendonly no/appendonly yes/' /etc/redis/redis.conf
    sudo sed -i 's/^appendfsync everysec/appendfsync always/' /etc/redis/redis.conf
    sudo sed -i 's/^supervised no$/supervised auto/' /etc/redis/redis.conf
    sudo service redis-server restart
    if redis-cli ping; then
        echo "Redis has been installed and configured successfully."
    else
        echo "Failed to install or configure Redis"
        exit 1
    fi
else
    echo "Redis is already installed"
fi

# Install Git if not already installed
if ! command -v git &> /dev/null; then
    sudo apt install -y git
else
    echo "Git is already installed"
fi

# Clone the TinyDFS repository if it doesn't exist
if [ ! -d "${HOME_DIR}/TinyDFS" ]; then
    cd ${HOME_DIR}
    git clone https://github.com/ZhenbangYou/TinyDFS.git
    echo "Downloaded Github Repo."
else
    echo "Github Repo already downloaded"
fi

# Navigate to the TinyDFS directory
cd ${HOME_DIR}/TinyDFS

git config --global --add safe.directory ${HOME_DIR}/TinyDFS

# Pull the latest changes from the main branch
git pull origin main

# Build the project
bash scripts/build.sh

# Create necessary directories if they don't exist
mkdir -p logs

# Set the internal IP address
NAMENODE_IP="10.138.0.3"
LOG_FILE="logs/namenode.log"

# Delete the previous log file
if [ -f "${LOG_FILE}" ]; then
    rm ${LOG_FILE}
fi

# Kill any existing namenode processes
pkill -SIGTERM -f "namenode"

# Start the namenode
./namenode/namenode ${NAMENODE_IP}:8000 5 ${LOG_FILE}