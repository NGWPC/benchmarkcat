#!/bin/bash
################################################################################
# BenchmarkCat STAC - OWP Standalone Bootstrap Script
#
# Purpose: Self-contained setup script
# Instance Type: t3.xlarge (4 vCPU, 16 GB RAM)
# OS: Ubuntu 22.04 or Ubuntu 24.04
#
# Usage as AWS Console User Data:
#   1. Copy entire contents of this file
#   2. Paste into "User data" field during EC2 instance launch
#   3. Launch instance - script runs automatically on first boot
#
# Usage as manual script:
#   1. SSH to instance
#   2. Copy this file to instance
#   3. Run: sudo bash owp-bootstrap-standalone.sh
#
################################################################################

set -euxo pipefail

# Configuration Variables - CUSTOMIZE THESE FOR YOUR ENVIRONMENT
INSTALL_DIR="/opt/benchmarkcat"
LOG_DIR="/var/log/benchmarkcat"
BACKUP_DIR="/opt/backups/postgres"

# AWS Configuration - UPDATE THESE
AWS_REGION="us-east-1"
S3_BUCKET="owp-benchmark"  # Change to your OWP S3 bucket name
S3_CATALOG_PATH="stac/"              # Change based on your S3 structure
S3_PREFIX= 

# Database Configuration
POSTGRES_USER="pgstac"
POSTGRES_DB="stacdb"
POSTGRES_PASSWORD=""  # Will be auto-generated if left empty

# Docker Image Versions
PGSTAC_VERSION="v0.8.6"
STAC_API_VERSION="latest"
STAC_BROWSER_VERSION="latest"

################################################################################
# DO NOT EDIT BELOW THIS LINE UNLESS YOU KNOW WHAT YOU'RE DOING
################################################################################

# Detect OS version
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS_VERSION=$VERSION_ID
else
    echo "Cannot detect OS version. Exiting."
    exit 1
fi

echo "================================="
echo "BenchmarkCat STAC Bootstrap"
echo "Ubuntu Version: $OS_VERSION"
echo "Timestamp: $(date)"
echo "================================="

################################################################################
# Logging Setup
################################################################################
mkdir -p $LOG_DIR
exec 1> >(tee -a "$LOG_DIR/bootstrap.log")
exec 2>&1
echo "[$(date)] Bootstrap started"

################################################################################
# System Updates and Package Installation
################################################################################
echo "[$(date)] Updating system packages..."

# Ensure IPv4-only apt (fixes Docker + mirror issues)
echo 'Acquire::ForceIPv4 "true";' > /etc/apt/apt.conf.d/99force-ipv4

# Clean apt state completely
sudo apt-get clean
sudo rm -rf /var/lib/apt/lists/*

# Ubuntu package installation
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install -y \
    docker.io \
    git \
    htop \
    vim \
    wget \
    curl \
    jq \
    postgresql-client \
    python3 \
    python3-pip

curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o awscliv2.zip
sudo apt-get install -y unzip
unzip -o awscliv2.zip
sudo ./aws/install --update
aws --version

# Install Python dependencies for catalog loading scripts
echo "[$(date)] Installing Python dependencies..."
pip3 install --no-cache-dir --break-system-packages psycopg2-binary

echo "[$(date)] System packages installed"

################################################################################
# Docker Installation and Configuration
################################################################################
echo "[$(date)] Configuring Docker..."

# Conditional for local Docker - can remove for user-data
if command -v systemctl >/dev/null 2>&1; then
  systemctl enable docker
  systemctl start docker
fi


# Add ubuntu user to docker group
if id "ubuntu" &>/dev/null; then
    sudo usermod -a -G docker ubuntu
    RUNTIME_USER="ubuntu"
else
    RUNTIME_USER="root"
fi

sudo chmod 666 /var/run/docker.sock

docker --version

################################################################################
# Docker Compose Installation
################################################################################
if ! command -v docker-compose &> /dev/null; then
    echo "[$(date)] Installing Docker Compose..."

    sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
        -o /usr/local/bin/docker-compose

    sudo chmod +x /usr/local/bin/docker-compose
    sudo ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
fi

docker-compose --version

################################################################################
# Directory Structure
################################################################################
echo "[$(date)] Creating directory structure..."

sudo mkdir -p $INSTALL_DIR/deployment
sudo mkdir -p $INSTALL_DIR/scripts
sudo mkdir -p $LOG_DIR
sudo mkdir -p $BACKUP_DIR
sudo mkdir -p /opt/stac/logs

if [ "$RUNTIME_USER" != "root" ]; then
    # Change ownership of directories (ignore errors for read-only mounts like test environments)
    sudo chown -R $RUNTIME_USER:$RUNTIME_USER $INSTALL_DIR/deployment $LOG_DIR $BACKUP_DIR /opt/stac 2>/dev/null || true
    # Try to chown scripts directory, but ignore if it's read-only (e.g., mounted volume in testing)
    sudo chown -R $RUNTIME_USER:$RUNTIME_USER $INSTALL_DIR/scripts 2>/dev/null || true
fi

################################################################################
# Generate Secure Password
################################################################################
if [ -z "$POSTGRES_PASSWORD" ]; then
    echo "[$(date)] Generating secure database password..."
    POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-25)
    echo "$POSTGRES_PASSWORD" | sudo tee $INSTALL_DIR/.db_password > /dev/null
    sudo chmod 600 $INSTALL_DIR/.db_password
    echo "[$(date)] Database password saved to $INSTALL_DIR/.db_password"
fi

################################################################################
# Create .env File
################################################################################
echo "[$(date)] Creating environment configuration..."

cat > $INSTALL_DIR/deployment/.env <<EOF
################################################################################
# BenchmarkCat STAC - OWP Environment Configuration
# Generated: $(date)
################################################################################

# Database Configuration
POSTGRES_USER=$POSTGRES_USER
POSTGRES_PASSWORD=$POSTGRES_PASSWORD
POSTGRES_DB=$POSTGRES_DB
POSTGRES_HOST=database
POSTGRES_PORT=5432

# STAC API Configuration
STAC_API_TITLE=OWP BenchmarkCat STAC API
STAC_API_DESCRIPTION=Benchmark evaluation data catalog for NOAA OWP
API_PORT=8082
BROWSER_PORT=8080
# Docker Image Versions
PGSTAC_VERSION="v0.8.6"
STAC_API_VERSION="latest"
STAC_BROWSER_VERSION="latest"


# AWS Configuration
AWS_REGION=$AWS_REGION
AWS_S3_BUCKET=$S3_BUCKET
AWS_REQUEST_PAYER=requester

# GDAL VSI Configuration
VSI_CACHE=TRUE
VSI_CACHE_SIZE=1000000000
GDAL_CACHEMAX=512
GDAL_HTTP_MAX_RETRY=3
GDAL_HTTP_RETRY_DELAY=1
CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif,.gpkg,.json,.parquet
GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR
CPL_VSIL_CURL_USE_HEAD=NO

# Application Performance
WEB_CONCURRENCY=10
DB_MIN_CONN_SIZE=5
DB_MAX_CONN_SIZE=50

# Logging
LOG_LEVEL=INFO
EOF

################################################################################
# Create Docker Compose File
################################################################################
echo "[$(date)] Creating docker-compose configuration..."
echo "[$(date)] BOOTSTRAP_TEST_MODE=${BOOTSTRAP_TEST_MODE:-false}"

cat > $INSTALL_DIR/deployment/docker-compose.yml <<'COMPOSE_EOF'
version: '3.8'

services:
  database:
    container_name: benchmarkcat-db
    image: ghcr.io/stac-utils/pgstac:${PGSTAC_VERSION}
    environment:
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - POSTGRES_DB=${POSTGRES_DB}
      - PGUSER=${POSTGRES_USER}
      - PGPASSWORD=${POSTGRES_PASSWORD}
      - PGDATABASE=${POSTGRES_DB}
    ports:
      - "5432:5432"
    volumes:
      - ./pgdata:/var/lib/postgresql/data
    command:
      - "postgres"
      - "-c"
      - "shared_buffers=2GB"
      - "-c"
      - "effective_cache_size=6GB"
      - "-c"
      - "work_mem=32MB"
      - "-c"
      - "random_page_cost=1.1"
      - "-c"
      - "effective_io_concurrency=200"
      - "-c"
      - "max_connections=100"
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 5

  stac-api:
    container_name: benchmarkcat-api
    image: ghcr.io/stac-utils/stac-fastapi-pgstac:${STAC_API_VERSION}
    environment:
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASS=${POSTGRES_PASSWORD}
      - POSTGRES_DBNAME=${POSTGRES_DB}
      - POSTGRES_HOST_READER=database
      - POSTGRES_HOST_WRITER=database
      - POSTGRES_PORT=5432
      - WEB_CONCURRENCY=${WEB_CONCURRENCY}
      - DB_MIN_CONN_SIZE=${DB_MIN_CONN_SIZE}
      - DB_MAX_CONN_SIZE=${DB_MAX_CONN_SIZE}
      - AWS_REGION=${AWS_REGION}
      - AWS_REQUEST_PAYER=${AWS_REQUEST_PAYER}
      - VSI_CACHE=${VSI_CACHE}
      - VSI_CACHE_SIZE=${VSI_CACHE_SIZE}
      - GDAL_CACHEMAX=${GDAL_CACHEMAX}
      - GDAL_HTTP_MAX_RETRY=${GDAL_HTTP_MAX_RETRY}
      - CPL_VSIL_CURL_ALLOWED_EXTENSIONS=${CPL_VSIL_CURL_ALLOWED_EXTENSIONS}
      - GDAL_DISABLE_READDIR_ON_OPEN=${GDAL_DISABLE_READDIR_ON_OPEN}
      - CPL_VSIL_CURL_USE_HEAD=${CPL_VSIL_CURL_USE_HEAD}
    ports:
      - "8082:8082"
    depends_on:
      database:
        condition: service_healthy
    restart: unless-stopped
    command: ["uvicorn", "stac_fastapi.pgstac.app:app", "--host", "0.0.0.0", "--port", "8082"]

  stac-browser:
    container_name: benchmarkcat-browser
    image: ghcr.io/radiantearth/stac-browser:${STAC_BROWSER_VERSION}
    environment:
      - SB_catalogUrl=http://0.0.0.0:8082
      - SB_maxPreviewsOnMap=0
    ports:
      - "8080:8080"
    depends_on:
      - stac-api
    restart: unless-stopped
COMPOSE_EOF

# Modify docker-compose.yml for test mode (Docker-in-Docker)
if [ "${BOOTSTRAP_TEST_MODE:-false}" = "true" ]; then
    echo "[$(date)] Test mode detected - converting to named volumes for Docker-in-Docker compatibility"
    # Replace bind mount with named volume
    sed -i 's|./pgdata:/var/lib/postgresql/data|pgstac-data:/var/lib/postgresql/data|g' $INSTALL_DIR/deployment/docker-compose.yml

    # Add all services to external network and volumes
    cat >> $INSTALL_DIR/deployment/docker-compose.yml <<'TEST_CONFIG_EOF'

networks:
  default:
    name: benchmarkcat-test
    external: true

volumes:
  pgstac-data:
    driver: local
TEST_CONFIG_EOF
else
    echo "[$(date)] Production mode - using bind mount at ./pgdata"
fi

################################################################################
# Create Health Check Script
################################################################################
echo "[$(date)] Creating health check script..."

cat > $INSTALL_DIR/deployment/health-check.sh <<'HEALTH_EOF'
#!/bin/bash

echo "=== BenchmarkCat STAC Health Check ==="
echo "Date: $(date)"
echo ""

# Check Docker containers
echo "--- Docker Containers ---"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

# Check API endpoint
echo "--- STAC API Health ---"
API_RESPONSE=$(curl -s http://localhost:8082/ 2>/dev/null)
if [ $? -eq 0 ]; then
    echo "$API_RESPONSE" | jq -r '.title // "API Running (no title)"' 2>/dev/null || echo "API: Running"
else
    echo "API: NOT RESPONDING"
fi
echo ""

# Check Browser endpoint
echo "--- STAC Browser Health ---"
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:8080 2>/dev/null || echo "Browser: NOT RESPONDING"
echo ""

# Check database
echo "--- Database Health ---"
docker exec benchmarkcat-db pg_isready -U pgstac -d stacdb 2>/dev/null || echo "Database: NOT READY"
echo ""

# Check disk usage
echo "--- Disk Usage ---"
df -h / | tail -1
echo ""

# Check memory
echo "--- Memory Usage ---"
free -h | grep Mem
echo ""

# Check CPU load
echo "--- CPU Load ---"
uptime
echo ""

# Check S3 access (if AWS credentials available)
echo "--- S3 Access Test ---"
if aws sts get-caller-identity &>/dev/null; then
    echo "AWS Credentials: OK"
    aws s3 ls s3://owp-benchmark/ --max-items 1 &>/dev/null && echo "S3 Access: OK" || echo "S3 Access: FAILED"
else
    echo "AWS Credentials: NOT CONFIGURED"
fi
echo ""

echo "=== Health Check Complete ==="
HEALTH_EOF

chmod +x $INSTALL_DIR/deployment/health-check.sh

################################################################################
# Create Backup Script
################################################################################
echo "[$(date)] Creating backup script..."

cat > $INSTALL_DIR/deployment/backup-db.sh <<'BACKUP_EOF'
#!/bin/bash
set -e

BACKUP_DIR=/opt/backups/postgres
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="stacdb_${TIMESTAMP}.sql.gz"
S3_BUCKET=$S3_BUCKET #S3_BUCKET="owp-benchmark"
S3_PREFIX="backups/stac-db"

mkdir -p $BACKUP_DIR

echo "[$(date)] Starting database backup..."

# Dump database and compress
docker exec benchmarkcat-db pg_dump -U pgstac -d stacdb | gzip > "$BACKUP_DIR/$BACKUP_FILE"

if [ -s "$BACKUP_DIR/$BACKUP_FILE" ]; then
    echo "[$(date)] Backup created: $BACKUP_FILE ($(du -h $BACKUP_DIR/$BACKUP_FILE | cut -f1))"

    # Upload to S3 (requires IAM role with S3 write permissions)
    if aws s3 cp "$BACKUP_DIR/$BACKUP_FILE" "s3://$S3_BUCKET/$S3_PREFIX/$BACKUP_FILE" 2>/dev/null; then
        echo "[$(date)] Backup uploaded to S3: s3://$S3_BUCKET/$S3_PREFIX/$BACKUP_FILE"
    else
        echo "[$(date)] Warning: Could not upload to S3 (check IAM permissions)"
    fi

    # Cleanup old local backups (keep last 7 days)
    find $BACKUP_DIR -name "stacdb_*.sql.gz" -mtime +7 -delete
    echo "[$(date)] Old backups cleaned up (kept last 7 days)"
else
    echo "[$(date)] ERROR: Backup file is empty"
    exit 1
fi
BACKUP_EOF

chmod +x $INSTALL_DIR/deployment/backup-db.sh

################################################################################
# Create Service Management Scripts
################################################################################
echo "[$(date)] Creating service management scripts..."

cat > $INSTALL_DIR/deployment/restart-services.sh <<'RESTART_EOF'
#!/bin/bash
cd /opt/benchmarkcat/deployment
docker-compose restart
echo "Services restarted. Run health-check.sh to verify."
RESTART_EOF

cat > $INSTALL_DIR/deployment/stop-services.sh <<'STOP_EOF'
#!/bin/bash
cd /opt/benchmarkcat/deployment
docker-compose down
echo "Services stopped."
STOP_EOF

cat > $INSTALL_DIR/deployment/start-services.sh <<'START_EOF'
#!/bin/bash
cd /opt/benchmarkcat/deployment
docker-compose up -d
echo "Services started. Run health-check.sh to verify."
START_EOF

cat > $INSTALL_DIR/deployment/view-logs.sh <<'LOGS_EOF'
#!/bin/bash
cd /opt/benchmarkcat/deployment
docker-compose logs -f --tail=100
LOGS_EOF

chmod +x $INSTALL_DIR/deployment/*.sh

################################################################################
# Pull Docker Images
################################################################################
echo "[$(date)] Pulling Docker images (this may take several minutes)..."

docker pull ghcr.io/stac-utils/pgstac:$PGSTAC_VERSION
docker pull ghcr.io/stac-utils/stac-fastapi-pgstac:$STAC_API_VERSION
docker pull ghcr.io/radiantearth/stac-browser:$STAC_BROWSER_VERSION

echo "[$(date)] Docker images pulled successfully"

################################################################################
# Start Services
################################################################################
echo "[$(date)] Starting BenchmarkCat services..."

# Ensure log directories exist and are writable (For Testing (Docker-in-Docker))
sudo mkdir -p /opt/stac/logs
sudo chmod -R 777 /opt/stac/logs

cd $INSTALL_DIR/deployment
docker-compose up -d

echo "[$(date)] Waiting for database initialization (30 seconds)..."
sleep 30

################################################################################
# Verify Database
################################################################################
echo "[$(date)] Verifying database extensions..."

docker exec -i benchmarkcat-db psql -U $POSTGRES_USER -d $POSTGRES_DB -c "\dx" 2>&1 | grep -q "pgstac" && \
    echo "[$(date)] pgstac extension verified" || \
    echo "[$(date)] Warning: pgstac extension not found (may need manual initialization)"

################################################################################
# Configure Systemd Service
################################################################################
# Only configure systemd if it's available (not in Docker containers)
if command -v systemctl >/dev/null 2>&1; then
    echo "[$(date)] Creating systemd service for auto-start..."

    sudo cat > /etc/systemd/system/benchmarkcat.service <<'SYSTEMD_EOF'
[Unit]
Description=BenchmarkCat STAC Services
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/benchmarkcat/deployment
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
SYSTEMD_EOF

    sudo systemctl daemon-reload
    sudo systemctl enable benchmarkcat.service
    echo "[$(date)] Systemd service configured"
else
    echo "[$(date)] Systemd not available (skipping service configuration)"
fi

################################################################################
# Configure Automated Backups
################################################################################
# Only configure cron if it's available (not in Docker containers)
if command -v crontab >/dev/null 2>&1; then
    echo "[$(date)] Configuring automated backups..."

    # Add weekly backup to crontab (Sunday 2 AM)
    if [ "$RUNTIME_USER" != "root" ]; then
        sudo -u $RUNTIME_USER bash -c "(crontab -l 2>/dev/null; echo '0 2 * * 0 $INSTALL_DIR/deployment/backup-db.sh >> $LOG_DIR/backup.log 2>&1') | crontab -"
    else
        (crontab -l 2>/dev/null; echo "0 2 * * 0 $INSTALL_DIR/deployment/backup-db.sh >> $LOG_DIR/backup.log 2>&1") | crontab -
    fi

    echo "[$(date)] Automated weekly backups configured (Sunday 2 AM)"
else
    echo "[$(date)] Cron not available (skipping automated backup configuration)"
fi

################################################################################
# Configure Firewall (if applicable)
################################################################################
if command -v firewall-cmd &> /dev/null; then
    echo "[$(date)] Configuring firewall..."
    sudo firewall-cmd --permanent --add-port=8082/tcp
    sudo firewall-cmd --permanent --add-port=8080/tcp
    sudo firewall-cmd --reload
    echo "[$(date)] Firewall configured"
fi

################################################################################
# Create README
################################################################################
cat > $INSTALL_DIR/README.txt <<'README_EOF'
BenchmarkCat STAC Deployment
============================

Installation Directory: /opt/benchmarkcat
Logs Directory:        /var/log/benchmarkcat

Services:
- STAC API:     http://<instance-ip>:8082
- STAC Browser: http://<instance-ip>:8080
- Database:     localhost:5432 (internal only)

Database Credentials:
- User:     pgstac
- Password: See /opt/benchmarkcat/.db_password
- Database: stacdb

Quick Commands:
---------------
Health Check:    /opt/benchmarkcat/deployment/health-check.sh
View Logs:       /opt/benchmarkcat/deployment/view-logs.sh
Restart:         /opt/benchmarkcat/deployment/restart-services.sh
Stop:            /opt/benchmarkcat/deployment/stop-services.sh
Start:           /opt/benchmarkcat/deployment/start-services.sh
Manual Backup:   /opt/benchmarkcat/deployment/backup-db.sh

Test API:
---------
curl http://localhost:8082/
curl http://localhost:8082/collections

Test STAC Browser:
curl http://localhost:8080/

Loading Catalog Data:
---------------------
# 1. Clone the benchmarkcat repository to get loading scripts
cd /opt/benchmarkcat
git clone https://github.com/NGWPC/benchmarkcat.git repo
# Or copy scripts from your local repository

# 2. Dry run to preview what would be loaded
python3 repo/deployment/scripts/load_catalog.py /path/to/catalog --db-host database --dry-run

# 3. Load STAC catalog
python3 repo/deployment/scripts/load_catalog.py /path/to/catalog --db-host database

Database Access:
----------------
docker exec -it benchmarkcat-db psql -U pgstac -d stacdb

Common SQL Queries:
  \dt                              # List tables
  \dx                              # List extensions
  SELECT COUNT(*) FROM pgstac.items;      # Count items
  SELECT collection, COUNT(*) FROM pgstac.items GROUP BY collection;

Automated Backups:
------------------
Schedule:  Weekly (Sunday 2 AM)
Location:  s3://owp-benchmark/backups/stac-db/
Local:     /opt/backups/postgres/ (last 7 days)

Troubleshooting:
----------------
If services not running:
  docker ps
  /opt/benchmarkcat/deployment/view-logs.sh
  /opt/benchmarkcat/deployment/restart-services.sh

If API not responding:
  curl http://localhost:8082/
  docker logs benchmarkcat-api

If database issues:
  docker logs benchmarkcat-db
  docker exec benchmarkcat-db pg_isready -U pgstac -d stacdb
README_EOF

################################################################################
# Final Health Check
################################################################################
echo ""
echo "================================="
echo "Waiting for services to stabilize..."
echo "================================="
sleep 10

$INSTALL_DIR/deployment/health-check.sh

################################################################################
# Print Summary
################################################################################
INSTANCE_IP=$(ec2-metadata --public-ipv4 2>/dev/null | cut -d " " -f 2 || echo "<instance-ip>")

cat <<SUMMARY

================================================================================
BenchmarkCat STAC Deployment - COMPLETE
================================================================================

Installation Directory: $INSTALL_DIR
Logs Directory:         $LOG_DIR
Backup Directory:       $BACKUP_DIR

Services (verify with health-check.sh):
  STAC API:     http://$INSTANCE_IP:8082
  STAC Browser: http://$INSTANCE_IP:8080
  Database:     localhost:5432 (internal)

Database Credentials:
  User:     $POSTGRES_USER
  Password: (stored in $INSTALL_DIR/.db_password)
  Database: $POSTGRES_DB

Quick Start:
  Health Check:  $INSTALL_DIR/deployment/health-check.sh
  View Logs:     $INSTALL_DIR/deployment/view-logs.sh
  Test API:      curl http://localhost:8082/

Next Steps:
  1. Run health check: $INSTALL_DIR/deployment/health-check.sh
  2. Test API endpoint: curl http://localhost:8082/
  3. Access STAC Browser: http://$INSTANCE_IP:8080
  4. Clone repo for catalog loading scripts (see README.txt)
  5. Load STAC catalog data (see repo/deployment/scripts/USAGE_INSTRUCTIONS.md)
  6. Configure IAM role for S3 access (if not already done)

Documentation: $INSTALL_DIR/README.txt

Automated Features:
  - Services auto-start on boot (systemd)
  - Weekly database backups (Sunday 2 AM)
  - 7-day local backup retention

================================================================================

SUMMARY

echo "[$(date)] Bootstrap completed successfully!"
echo "[$(date)] Full log available at: $LOG_DIR/bootstrap.log"
