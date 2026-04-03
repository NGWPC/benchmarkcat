# BenchmarkCat STAC: Consolidated Migration & Deployment Runbook

## Context

BenchmarkCat is a STAC geospatial catalog (~23,800 items, 8 collections, ~1.5 TB assets) currently hosted in NGWPC's `fimc-data` S3 bucket. This runbook consolidates existing documentation (`Deployment_Guide.txt`, `Deployment_Strategy_Overview_OWP.md`, `s3_migration/README.md`, `terraform/README.md`) into a single executable plan for migrating to OWP's infrastructure: new S3 bucket (`owp-benchmark`), EC2 with Docker stack (pgSTAC, STAC API, STAC Browser, asset-proxy), Terraform-managed infrastructure, and full validation.

---

## Phase 0: Prerequisites & Cross-Team Coordination

### 0.1 Gather OWP Environment Details
- AWS Account ID, preferred region (`us-east-1`)
- VPC name, private subnet name pattern
- Route53 hosted zone ID
- SSH key pair name
- Session Manager logging policy ARN

### 0.2 Cross-Account IAM Setup

**OWP side** -- create a temporary migration role:
- Role name: `owp-benchmarkcat-migration-role` (EC2 trust policy)
- Attached policy 1 (source read): `s3:GetObject` and `s3:ListBucket` on `s3://fimc-data/benchmark/*` and `s3://fimc-data/hand_fim/test_cases/*`
- Attached policy 2 (dest write): `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` on `s3://owp-benchmark/*`

**NGWPC side** -- If necessary, update `fimc-data` bucket policy to grant cross-account read access.

### 0.3 Verify Cross-Account Access
```bash
aws sts get-caller-identity
aws s3 ls s3://fimc-data/benchmark/ | head -5
aws s3 ls s3://fimc-data/benchmark/stac-bench-cat/ | head -5
```

### 0.4 Create Destination Bucket (if needed)
```bash
aws s3 mb s3://owp-benchmark --region us-east-1
```

**Gate:** Do not proceed until cross-account S3 read is confirmed.

---

## Phase 1: Terraform Infrastructure

### 1.1 Create Configuration

Working dir: `deployment/terraform/`

Create `terraform.tfvars` (template in `deployment/terraform/README.md`):
```hcl
environment        = "test"
aws_region         = "us-east-1"
api_name           = "benchmarkcat"
hosted_zone_id     = "<ZONE_ID>"
session_manager_logging_policy_arn = "<SSM_POLICY_ARN>"
vpc_name             = "<VPC_NAME>"
subnet_name_pattern  = "<SUBNET_PATTERN>*"
instance_type        = "t3.xlarge"
root_volume_size     = 100
enterprise_mode      = false
s3_read_paths        = ["owp-benchmark/*"]
s3_write_paths       = ["owp-benchmark/backups/*"]
backup_s3_uri        = "s3://owp-benchmark/backups/stac-db/"
log_retention_days   = 7
```

Create `backend.tf` for remote state (S3 backend recommended).

Key Terraform files:
- `deployment/terraform/main.tf` -- IAM roles, security groups, EC2, optional ALB/ASG
- `deployment/terraform/variables.tf` -- all configurable inputs (60+ variables)
- `deployment/terraform/data.tf` -- VPC/subnet/AMI lookups
- `deployment/terraform/outputs.tf` -- API URL, instance IP, SSH instructions

### 1.2 Deploy
```bash
cd deployment/terraform
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

Creates: Security group (8080/8082/8083 + SSH to VPC), IAM role with dynamic S3 policies, EC2 instance with bootstrap, Route53 A record, CloudWatch log group.

### 1.3 Verify Bootstrap

The bootstrap is automated via `deployment/terraform/templates/user_data_standalone.sh.tpl` (or can be run manually with `deployment/terraform/user-data/owp-bootstrap.sh`). It installs Docker, Docker Compose, creates the 4-container stack, systemd service, and cron backups.

**Note:** The bootstrap generates utility scripts (`health-check.sh`, `backup-db.sh`, `restart-services.sh`) on the EC2 instance at `/opt/benchmarkcat/deployment/`. These are not present in the repository.

```bash
terraform output standalone_instance_ip
ssh -i ~/.ssh/owp-benchmarkcat-key.pem ubuntu@<ec2-ip>

cat /var/log/benchmarkcat/bootstrap.log
/opt/benchmarkcat/deployment/health-check.sh
docker ps  # Expect: benchmarkcat-db, benchmarkcat-api, benchmarkcat-browser, benchmarkcat-asset-proxy
```

**Rollback:** `terraform destroy -var-file="terraform.tfvars"`

**State after Phase 1:** 4 containers running, empty database, API on 8082, Browser on 8080, proxy on 8083.

### 1.4 Clone Repository

Scripts referenced in later phases live in this repo. Clone it to the expected path on the EC2 instance:

```bash
# Use owp-deployment branch (or main if already merged)
sudo git clone https://github.com/NGWPC/benchmarkcat.git /opt/benchmarkcat/repo -b owp-deployment
```

Verify:
```bash
ls /opt/benchmarkcat/repo/deployment/scripts/
# Expected: load_catalog.py, rewrite_asset_urls.py, test_asset_proxy.sh, reset_database.sh, etc.
```

---

## Phase 2: S3 Migration

Script: `deployment/s3_migration/migrate_s3.py`
Reference: `deployment/s3_migration/S3_README.md`

### 2.1 Dry Run
```bash
cd /opt/benchmarkcat/repo/deployment/s3_migration

python3 migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket owp-benchmark \
  --dry-run --verbose
```
Verify all 8 path mappings displayed (defined in `migrate_s3.py` `PATH_MAPPINGS`):

| Collection | Source | Destination |
|---|---|---|
| ble-collection | `benchmark/high_resolution_validation_data_ble` | `data/ble-collection` |
| ripple-fim-collection | `benchmark/ripple_fim_100` | `data/ripple-fim-collection` |
| hwm-collection | `benchmark/high_water_marks/usgs` | `data/hwm-collection` |
| nws-fim-collection | `hand_fim/test_cases/nws_test_cases/validation_data_nws` | `data/nws-fim-collection` |
| usgs-fim-collection | `hand_fim/test_cases/usgs_test_cases/validation_data_usgs` | `data/usgs-fim-collection` |
| gfm-collection | `benchmark/rs/gfm` | `data/gfm-collection` |
| iceye-collection | `benchmark/rs/iceye` | `data/iceye-collection` |
| gfm-expanded-collection | `benchmark/rs/PI4` | `data/gfm-expanded-collection` |
| STAC catalog | `benchmark/stac-bench-cat` | `stac/` |

### 2.2 Execute Migration

```bash
# Download catalog + update HREFs + generate copy script
python3 migrate_s3.py \
  --source-bucket fimc-data --dest-bucket owp-benchmark \
  --generate-copy-commands --skip-upload

# Review sample HREF
cat ~/benchmark-catalog/dest_catalog/gfm-collection/items/*/item.json | jq '.assets[].href' | head -5
# Expected: s3://owp-benchmark/data/gfm-collection/...

# Review migration manifest (all HREF changes logged for auditing)
jq 'length' ~/benchmark-catalog/migration_manifest.json

# Copy assets
~/benchmark-catalog/copy_assets.sh

# Monitor in another terminal:
watch -n 30 'aws s3 ls s3://owp-benchmark/data/ --recursive | wc -l'

# Upload updated catalog
python3 migrate_s3.py \
  --source-bucket fimc-data --dest-bucket owp-benchmark \
  --skip-download --skip-update
```

### 2.3 Verify Migration
```bash
aws s3 ls s3://owp-benchmark/stac/ --recursive | wc -l    # ~22,000
aws s3 ls s3://owp-benchmark/data/ --recursive | wc -l
aws s3 cp s3://owp-benchmark/stac/catalog.json - | jq '.'
aws s3 ls s3://owp-benchmark/data/                          # 8 collection dirs
```

### 2.4 Finalize Bucket Configuration

**Enable S3 Versioning**
Enable versioning on the bucket to protect the STAC metadata from accidental overwrites or deletions. 
This is especially critical for the `stac/` prefix which contains the catalog's structural definitions.

```bash
aws s3api put-bucket-versioning \
  --bucket owp-benchmark \
  --versioning-configuration Status=Enabled
```

**Recovery:** `copy_assets.sh` is idempotent -- re-run on failure, it skips existing files.

---

## Phase 3: Catalog Loading (From EC2 Server)

Script: `deployment/scripts/load_catalog.py`

### 3.1 Sync Catalog Locally
```bash
aws s3 sync s3://owp-benchmark/stac/ ~/stac-catalog/ --exclude "*" --include "*.json"
```

### 3.2 Load to pgstac
```bash
export PGPASSWORD=$(sudo cat /opt/benchmarkcat/.db_password)

python3 /opt/benchmarkcat/repo/deployment/scripts/load_catalog.py \
  ~/stac-catalog --db-host localhost --db-password $PGPASSWORD --dry-run

python3 /opt/benchmarkcat/repo/deployment/scripts/load_catalog.py \
  ~/stac-catalog --db-host localhost --db-password $PGPASSWORD 
```

### 3.3 Verify
```bash
docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SELECT collection, COUNT(*) FROM pgstac.items GROUP BY collection ORDER BY collection;"

# Total ~23,000
docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SELECT COUNT(*) FROM pgstac.items;"

curl http://localhost:8082/collections | jq '.collections | length'
```

**Rollback:** Reset database with `deployment/scripts/reset_database.sh --force` and re-load.

---

## Phase 4: Asset URL Rewriting

Script: `deployment/scripts/rewrite_asset_urls.py`

### 4.1 Verify Proxy

The asset-proxy service (`deployment/asset-proxy/app.py`) streams S3 content using IAM role credentials with Range request support for COG rendering.

```bash
sudo /opt/benchmarkcat/repo/deployment/scripts/test_asset_proxy.sh
```

This tests: proxy health endpoint, AWS credentials, sample asset query from DB, direct S3 access, proxy URL serving.

### 4.2 Rewrite
```bash
export HOST_IP=$(hostname -I | awk '{print $1}')
export PGPASSWORD=$(cat /opt/benchmarkcat/.db_password)

python3 /opt/benchmarkcat/repo/deployment/scripts/rewrite_asset_urls.py \
  --proxy-url http://${HOST_IP}:8083 \
  --db-host localhost --db-password $PGPASSWORD --dry-run

python3 /opt/benchmarkcat/repo/deployment/scripts/rewrite_asset_urls.py \
  --proxy-url http://${HOST_IP}:8083 \
  --db-host localhost --db-password $PGPASSWORD
```

Transforms: `s3://owp-benchmark/data/...` -> `http://<HOST_IP>:8083/s3/owp-benchmark/data/...`

**Note:** Use private VPC IP for internal access, or domain name if DNS is configured for external users.

### 4.3 Verify
```bash
curl -s "http://${HOST_IP}:8082/collections/gfm-collection/items?limit=1" | \
  jq '.features[0].assets[].href'
# All should show http://<HOST_IP>:8083/s3/owp-benchmark/data/...
```

---

## Phase 5: Validation

### 5.1 Service Health
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
/opt/benchmarkcat/deployment/health-check.sh
```
All 4 containers running: `benchmarkcat-db`, `benchmarkcat-api`, `benchmarkcat-browser`, `benchmarkcat-asset-proxy`

### 5.2 API Endpoints
```bash
curl http://${HOST_IP}:8082/ | jq '.title'
curl http://${HOST_IP}:8082/conformance | jq '.conformsTo | length'
curl http://${HOST_IP}:8082/collections | jq '.collections[].id'
curl "http://${HOST_IP}:8082/search?limit=10" | jq '.features | length'
time curl -s "http://${HOST_IP}:8082/search?bbox=-90,30,-80,40&limit=10" > /dev/null  # < 500ms
```

### 5.3 Database Integrity
```bash
docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "SELECT collection, COUNT(*) FROM pgstac.items GROUP BY collection ORDER BY collection;"
```
Verify collection count matches expected (8 collections) and total item count matches source catalog (~22,800).

### 5.4 Asset Proxy & S3 Access
```bash
sudo /opt/benchmarkcat/repo/deployment/scripts/test_asset_proxy.sh
```

### 5.5 GDAL/COG Rendering
```bash
SAMPLE_URL=$(curl -s "http://${HOST_IP}:8082/collections/gfm-collection/items?limit=1" | \
  jq -r '.features[0].assets | .[keys[0]].href')
curl -s -I -H "Range: bytes=0-1023" "$SAMPLE_URL" | grep -i "content-range"
# Expect: HTTP 206 with Content-Range header
```

### 5.6 STAC Browser UI
Open `http://<ec2-ip>:8080`:
- All collections visible and browsable
- Individual items display correctly with metadata
- Asset links resolve (thumbnails load, download links work)

### 5.7 QGIS Integration
- Connect QGIS to `http://<ec2-ip>:8082`
- Load a raster layer from a collection
- Verify data renders correctly

### 5.8 Performance
```bash
ab -n 1000 -c 10 http://${HOST_IP}:8082/collections
# No errors, mean response < 500ms
```

### 5.9 Monitoring & Ops
```bash
systemctl status benchmarkcat
crontab -l | grep backup-db.sh          # Sunday 2 AM
sudo /opt/benchmarkcat/deployment/backup-db.sh
ls -lh /opt/backups/postgres/
```

---

## Phase 6: Post-Deployment

### 6.1 Update .env
The `.env` file is generated during bootstrap at `/opt/benchmarkcat/deployment/.env`. If `S3_BUCKET` is empty:
```bash
# Set the destination bucket
sed -i 's/^S3_BUCKET=.*/S3_BUCKET=owp-benchmark/' /opt/benchmarkcat/deployment/.env
sudo /opt/benchmarkcat/deployment/restart-services.sh
```

### 6.2 Initial Backup
```bash
sudo /opt/benchmarkcat/deployment/backup-db.sh
```

### 6.3 Sync Project Documentation to S3 (Optional)

```bash
mkdir -p /opt/benchmarkcat/repo/docs/
cp /opt/benchmarkcat/repo/deployment/*.md /opt/benchmarkcat/repo/docs/
cp /opt/benchmarkcat/repo/deployment/*.txt /opt/benchmarkcat/repo/docs/
aws s3 sync /opt/benchmarkcat/repo/docs/ s3://owp-benchmark/docs/
```

### 6.4 Record Final Config
```
EC2 IP:        terraform output standalone_instance_ip
STAC API:      http://<ip>:8082
STAC Browser:  http://<ip>:8080
Asset Proxy:   http://<ip>:8083
SSH:           ssh -i ~/.ssh/owp-benchmarkcat-key.pem ubuntu@<ip>
DB Password:   /opt/benchmarkcat/.db_password
```

---

## Dependency Graph

```
Phase 0 (IAM/Coordination)
    |
    v
Phase 1 (Terraform)
    |
    v
Phase 2 (S3 Migration)
    |
    v
Phase 3 (Catalog Loading)
    |
    v
Phase 4 (URL Rewriting)
    |
    v
Phase 5 (Validation)
    |
    v
Phase 6 (Post-Deployment)
```

## Key Files

| File | Role |
|---|---|
| `deployment/terraform/main.tf` | IAM, SG, EC2, ALB resources |
| `deployment/terraform/variables.tf` | All configurable inputs |
| `deployment/terraform/data.tf` | VPC, subnet, AMI lookups |
| `deployment/terraform/outputs.tf` | API URL, instance IP, SSH instructions |
| `deployment/terraform/templates/user_data_standalone.sh.tpl` | Bootstrap template (used by Terraform) |
| `deployment/terraform/user-data/owp-bootstrap.sh` | Bootstrap script (manual execution) |
| `deployment/s3_migration/migrate_s3.py` | Core migration with PATH_MAPPINGS |
| `deployment/s3_migration/S3_README.md` | Detailed migration guide |
| `deployment/scripts/load_catalog.py` | pgstac catalog loader |
| `deployment/scripts/rewrite_asset_urls.py` | S3 -> proxy URL rewriter |
| `deployment/scripts/test_asset_proxy.sh` | Proxy validation |
| `deployment/scripts/reset_database.sh` | Database reset utility |
| `deployment/scripts/Scripts_README.md` | Script documentation |
| `deployment/asset-proxy/app.py` | FastAPI S3 streaming proxy |
| `deployment/Deployment_Guide.txt` | Original deployment guide |
| `deployment/Deployment_Strategy_Overview_OWP.md` | Architecture & cost analysis |
