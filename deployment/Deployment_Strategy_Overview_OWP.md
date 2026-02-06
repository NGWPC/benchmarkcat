# BenchmarkCat STAC Deployment Strategy for OWP

**Date:** January 22, 2026
**Author:** NGWPC FIMC Team
**For:** NOAA Office of Water Prediction (OWP)

---

## Overview

This document outlines the deployment strategy for BenchmarkCat STAC (SpatioTemporal Asset Catalog) in the OWP AWS infrastructure. This deployment is based on proven architectures from NGWPC internal environments and will provide OWP users with 24/7 access to 1.5 TB of geospatial benchmark assets.

**Note:** This is a new production deployment for OWP. NGWPC's internal OE and TEST environments will remain operational for continued development and testing.

### Key Objectives

- Deploy 24/7 STAC API in OWP AWS environment
- Provide access to 1.5 TB of geospatial benchmark assets via S3
- Enable easy access via STAC Browser and QGIS integration
- Minimize operational costs while maintaining performance

---

## Reference Deployments (NGWPC Internal)

The OWP deployment is based on two NGWPC internal environments that will remain operational:

### NGWPC OE (Reference Architecture 1)

**Storage:** Local disk storage at /efs/benchmark/bench_stac/ (1.5 TB)
**API:** stac-fastapi-pgstac (latest version)
**Database:** External RDS instance
**Access:** HTTP on ports 8082 (API), 8080 (Browser), 8000 (fileserver)
**Architecture:** 3-container deployment with dedicated file server

### NGWPC TEST (Reference Architecture 2)

**Storage:** PostgreSQL RDS references to S3
**API:** stac-fastapi-pgstac v4.0.3
**Database:** External RDS instance
**Access:** HTTP on ports 8000 (API), 8081 (Browser)
**Architecture:** Simplified 2-container deployment, no separate file server

---

## OWP Deployment Architecture

**Design Principles:**
- S3 as single source of truth (all 1.5 TB of benchmark assets)
- Self-contained docker-compose deployment (PostgreSQL + STAC API)
- Always-on availability (independent of developer EC2 lifecycle)
- Direct S3 asset access via GDAL Virtual File System (no file server needed)

**Components:**

Application Layer:
- STAC API (FastAPI) - Port 8082
- PostgreSQL with pgstac extension - Port 5432 (internal)
- STAC Browser - Port 8080

Storage Layer:
- S3 Bucket (fimc-data): 1.5 TB of geospatial assets
- STAC Catalog: 22,845 catalog files

Access Layer:
- OWP users: AWS SSO for S3, HTTP for STAC API
- QGIS integration: Direct STAC API connection

---

## Deployment Options

### Option A: Dedicated EC2 Instance (RECOMMENDED)

**Architecture:** Single t3.large EC2 instance running 24/7 with docker-compose stack

**Monthly Cost:** ~$125 (EC2 $60 + EBS $8 + S3 $35 + data transfer $10-15 + backups $1-2 + CloudWatch $5-10)

**Pros:**
- Simple deployment and management
- 40% cheaper than Fargate
- Easy troubleshooting
- Straightforward PostgreSQL backups

**Cons:**
- Single point of failure (mitigated with AMI backups)
- Manual OS patching
- Manual scaling if needed

**Best For:** Current OWP requirements (stable workload, cost-effective)

---

### Option B: ECS Fargate

**Architecture:** ECS Fargate tasks with RDS or EFS-backed PostgreSQL

**Monthly Cost:** ~$200 (Fargate $90-105 + EFS/RDS $10-15 + S3 $35 + ALB $25 + data transfer $15-20 + backups $1-2 + CloudWatch $5-10)

**Pros:**
- No server management
- Automatic high availability (multi-AZ)
- Auto-scaling capabilities

**Cons:**
- 60% more expensive
- More complex setup
- Steeper learning curve

**Best For:** Future growth scenarios with high availability requirements

---

## Storage Strategy

**S3-Only Approach (RECOMMENDED)**

| Storage Type | Monthly Cost (1.5 TB) | Notes |
|--------------|--------------|-------|
| S3 Standard | $35 | Recommended baseline |
| S3 Standard-IA (90+ days) | $19 | 45% savings for infrequent access |
| EFS Standard | $461 | 13x more expensive - NOT recommended |

**Why S3:**
- 93% cheaper than EFS ($35 vs $461/month)
- 99.999999999% durability
- Native GDAL VSI support (no file server needed)
- Unlimited scalability
- Lifecycle policies for automatic cost optimization
- GDAL caching (1GB) provides good performance

### **Proposed re-structuring of current S3 paths (example):**
```
  s3://owp-benchmark/
  ├── stac/                                    # STAC metadata (22,845 files, ~200MB)
  │   ├── catalog.json                         # Root catalog
  │   ├── collections/
  │   │   ├── gfm-collection/
  │   │   │   ├── collection.json
  │   │   │   └── items/
  │   │   │       ├── gfm-dfo-4336-20160307/
  │   │   │       │   └── gfm-dfo-4336-20160307.json
  │   │   │       └── ...
  │   │   ├── gfm-expanded-collection/
  │   │   ├── iceye-collection/
  │   │   ├── ripple-fim-collection/
  │   │   ├── ble-collection/
  │   │   ├── hwm-collection/
  │   │   └── usgs-fim-collection/
  │   └── assets/
  │       ├── WBDHU8_webproj.gpkg              # Shared HUC8 boundaries
  │       └── derived-asset-data/              # Parquet caches
  │           ├── gfm_collection.parquet
  │           ├── gfm_expanded_collection.parquet
  │           └── ...
  │
  ├── data/                                    # Geospatial assets (1.5 TB)
  │   ├── gfm/                                 # GFM flood products
  │   │   ├── dfo-4336/
  │   │   │   └── S1A_IW_GRDH_[...]/
  │   │   │       ├── *_ENSEMBLE_FLOOD_*.tif
  │   │   │       ├── *_ENSEMBLE_UNCERTAINTY_*.tif
  │   │   │       ├── *_ADVFLAG_*.tif
  │   │   │       └── ...
  │   │   └── ...
  │   ├── iceye/                               # ICEYE satellite imagery
  │   │   └── ICEYE_FSD-[...]/
  │   ├── ble/                                 # BLE validation data
  │   ├── ripple/                              # RIPPLE FIM
  │   ├── hwm/                                 # High water marks
  │   └── usgs/                                # USGS FIM
  │
  └── docs/                                    # Documentation
      ├── gfm_data_readme.pdf
      └── collection_metadata/
```
---

## Implementation Plan

### Phase 1: Pre-Deployment Testing (NGWPC TEST Account)
1. Provision AWS resources (EC2, security groups, IAM roles)
2. Deploy docker-compose stack
3. Load subset of STAC catalog (e.g., iceye collection)
4. Validate S3 asset accessibility via GDAL VSI
5. Document findings and create OWP deployment guide

### Phase 2: OWP Deployment
1. Verify NGWPC access to OWP AWS (or prepare handoff documentation)
2. Plan S3 data transfer (NGWPC S3 → OWP S3, or cross-account access)
3. Provision OWP AWS resources
4. Deploy docker-compose stack
5. Load full STAC catalog (22,845 files) to PostgreSQL
6. Validate S3 asset accessibility

### Phase 3: Validation & Launch
1. API health checks (/, /conformance, /collections, /search)
2. STAC Browser functionality testing
3. QGIS integration testing with OWP users
4. Performance validation (response times, concurrent users)
5. Configure backups and monitoring
6. Conduct OWP team training
7. Production launch

---

## Success Criteria

**Technical Validation:**
- STAC API endpoints functional (/, /conformance, /collections, /search)
- All collections visible in STAC Browser
- S3 assets accessible via GDAL VSI
- QGIS successfully connects and loads data
- Adequate response times and concurrent user support

**Operational Readiness:**
- PostgreSQL weekly backups configured
- CloudWatch monitoring and alerting active
- Complete documentation delivered to OWP team:
  - Architecture overview
  - Deployment scripts and docker-compose files
  - QGIS connection instructions
  - Operational runbook (maintenance, troubleshooting)
  - Disaster recovery procedures
- OWP team training completed

---

## Backup & Disaster Recovery

**PostgreSQL Database:**
- Weekly automated backups to S3
- 7-day local retention
- Database size: 5-10 GB (metadata only)
- Recovery time: < 30 minutes

**EC2 Instance:**
- Monthly/Weekly AMI snapshots
- Configuration in git repository
- Recovery time: < 1 hour from AMI

**S3 Assets (1.5 TB):**
- S3 native durability (99.999999999%)
- Optional: S3 versioning or cross-region replication
- Re-ingestion capability from original sources if needed

---

## Recommendations

**Primary:** Deploy Option A (EC2 + S3) for its simplicity, lower cost, and clear upgrade path to Fargate if needed.

**Additional:**
1. Start with S3 Standard; add lifecycle policies after establishing usage patterns
2. Automate monthly/weekly AMI backups via AWS Backup
3. Configure CloudWatch alerts for CPU, memory, and disk usage
4. Prioritize operational documentation for OWP team self-sufficiency
5. Conduct hands-on training with OWP users (QGIS, STAC Browser, troubleshooting)

---

## References & Resources

- stac-fastapi-pgstac: https://github.com/stac-utils/stac-fastapi-pgstac
- STAC Browser: https://github.com/radiantearth/stac-browser
- GDAL Virtual File Systems: https://gdal.org/user/virtual_file_systems.html
- AWS S3 Best Practices: https://docs.aws.amazon.com/AmazonS3/latest/userguide/best-practices.html

---
