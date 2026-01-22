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
- Support OWP users with deployment and documentation
- Provide access to 1.5 TB of geospatial benchmark assets via S3
- Enable easy access via STAC Browser and QGIS integration
- Minimize operational costs while maintaining performance
- Establish backup and disaster recovery procedures for OWP deployment

---

## Reference Deployments (NGWPC Internal)

The OWP deployment will be based on lessons learned from two NGWPC internal environments that serve as reference architectures. **These internal environments will remain operational** for NGWPC development and testing purposes.

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

### Key Improvements for OWP Deployment

Based on these reference deployments, the OWP deployment will feature:

- S3-native asset storage (no local file dependencies)
- Simplified architecture without separate file server
- Self-contained docker-compose deployment
- Standardized backup and monitoring procedures
- 24/7 availability independent of developer workflows

---

## OWP Deployment Architecture

### Design Principles

1. **S3 as Single Source of Truth:** All 1.5 TB of benchmark assets stored in S3
2. **Self-Contained Deployment:** PostgreSQL and STAC API in single docker-compose stack
3. **Always-On Availability:** Services independent of developer EC2 lifecycle
4. **GDAL Integration:** Direct S3 asset access via GDAL Virtual File System
5. **Cost-Effective Scalability:** Architecture supports future growth without major redesign

### Target Architecture for OWP

**Application Layer:**
- STAC API (FastAPI) - Port 8082
- PostgreSQL with pgstac extension - Port 5432 (internal)
- STAC Browser - Port 8080

**Storage Layer:**
- S3 Bucket (fimc-data): 1.5 TB of geospatial assets
- STAC Catalog: 22,845 catalog files (collections, items, metadata)
- Organized structure: benchmark/, rs/iceye/, rs/PI4/, etc.

**Access Layer:**
- OWP users: AWS SSO for S3 access, HTTP for STAC API
- OWP developer EC2s: Shut down nightly, connect to always-on STAC service
- QGIS integration: Direct STAC API connection for data visualization

---

## Deployment Options Comparison

### Option A: Dedicated Always-On EC2 Instance (RECOMMENDED)

**Architecture:** Single EC2 instance (t3.large) running 24/7 with Docker Compose stack

**Advantages:**
- Simple deployment and management
- Lower monthly cost (~$115-120/month)
- Easy troubleshooting and debugging
- Straightforward PostgreSQL backups
- Direct docker-compose deployment

**Considerations:**
- Single point of failure (mitigated with AMI backups and monitoring)
- Manual OS/security patching required
- Manual scaling if user count grows significantly

**Best For:** Current OWP requirements (stable workload)

---

### Option B: ECS Fargate with Task Scheduling

**Architecture:** ECS Fargate tasks with RDS or EFS-backed PostgreSQL

**Advantages:**
- No server management required
- Automatic high availability (multi-AZ)
- Built-in load balancing and auto-scaling
- Integrated AWS monitoring and logging
- Infrastructure as Code

**Considerations:**
- Higher cost (~$185-215/month)
- More complex initial setup
- Steeper learning curve
- More difficult debugging (CloudWatch logs)

**Best For:** Future growth scenarios (high availability requirements)

---

## Storage Strategy: S3 vs EFS

### S3-Only Approach (RECOMMENDED)

**Cost Comparison for 1.5 TB:**

| Storage Type | Monthly Cost | Difference |
|--------------|--------------|------------|
| S3 Standard | $35 | Baseline |
| S3 Standard-IA (after 90 days) | $19 | 46% savings |
| EFS Standard | $461 | 13x more expensive |

**Key Benefits:**
- 93% cheaper than EFS
- 99.999999999% durability (11 nines)
- No synchronization required
- Native GDAL support via Virtual File System
- Unlimited scalability
- Lifecycle policies for automatic archival

**Performance Considerations:**
- GDAL VSI caching (1GB cache) mitigates latency
- Optimized for large geospatial files
- Sufficient for current OWP user load
- Can provision AWS S3 File Gateway to improve performance if necessary

---

## Deployment Timeline

| Phase | Key Activities |
|-------|----------------|
| **1. Preparation** | Internal NGWPC Testing/validation, S3 access verification, OWP AWS infrastructure planning |
| **2. Environment Setup** | EC2 launch in OWP AWS Acct, Docker installation, configuration |
| **3. Data Loading** | Load catalog (22,845 files) metadata from S3 to PostgreSQL |
| **4. Testing & Validation** | API health checks, Browser testing, QGIS integration with OWP users |
| **5. Production Launch** | Documentation, user training, announce availability |
| **6. Monitoring Period** | Stability monitoring, performance tuning, user support |

---

## Success Criteria

The OWP deployment will be considered successful when all of the following are verified:

**Technical Criteria:**
- STAC API responding correctly on all endpoints (/, /conformance, /collections, /search)
- All collections visible in STAC Browser
- Assets accessible from S3 via GDAL (tested with sample files)
- QGIS successfully connects and loads raster data
- API response time is adequate for search queries
- Support for concurrent users without degradation

**Operational Criteria:**
- PostgreSQL automated backups running weekly
- Monitoring and alerting configured and tested
- Documentation complete and accessible to OWP team
- OWP team members trained on STAC access and usage

---

## Cost Summary

### Estimated Ongoing Monthly Costs

**Recommended Approach (EC2 + S3):**

| Item | Monthly Cost |
|------|-------------|
| Infrastructure (EC2, EBS, S3, data transfer) | $115-120 |
| Backup storage | $1-2 |
| Monitoring/alerting (CloudWatch) | $5-10 |
| **TOTAL MONTHLY** | **~$125** |

**Alternative Approach (ECS Fargate + S3):**

| Item | Monthly Cost |
|------|-------------|
| Infrastructure (Fargate, EFS, S3, ALB, data transfer) | $185-215 |
| Backup storage | $1-2 |
| Monitoring/alerting | $5-10 |
| **TOTAL MONTHLY** | **~$200** |

### Cost Optimization Opportunities

- S3 lifecycle policies: Move infrequently accessed data to Standard-IA after 90 days (~50% savings)
- Right-sizing: Start with t3.large, scale up only if needed based on monitoring
- Reserved Instances: 1-year commitment reduces EC2 costs by ~30% 

---

## Backup & Disaster Recovery

### Backup Strategy

**PostgreSQL Database:**
- Automated weekly backups
- 7-day local retention
- Continuous S3 backup synchronization
- Estimated database size: 5-10 GB (metadata only)
- Backup storage cost: ~$1-2/month

**EC2 Instance:**
- AMI snapshots
- Configuration stored in git repository
- Recovery time: < 1 hour from AMI

**S3 Assets (1.5 TB):**
- S3 versioning (optional, additional cost)
- Re-ingestion capability from original sources

---

## Next Steps

### Action items

1. **OWP Stakeholder Approval:** Review and approve this deployment strategy
2. **Test and Validate on NGWPC AWS TEST Account**: Confirm proposed strategy works, iron out kinks 
3. **OWP AWS Access:** Verify NGWPC access to provision resources in OWP AWS environment - S3, EC2, etc
4. **S3 Access:** Plan data transfer from NGWPC S3 to OWP S3 bucket. Confirm OWP AWS can access new benchmark data bucket

If 3. and 4. are not feasible, additional work with documentation and scripts will need to be written for OWP staff. 

### **TEST** Implementation Sequence (NGWPC)

1. Provision AWS resources in NGWPC TEST environment (EC2, security groups, IAM roles, etc)
2. Deploy Docker Compose stack in NGWPC TEST AWS
3. Load subset of STAC catalog metadata (just iceye?) from S3 to NGWPC PostgreSQL instance
4. Validate S3 asset accessibility from NGWPC environment
5. Create and validate documentation delivered to OWP

### Implementation Sequence (OWP)

1. Provision AWS resources in OWP environment (EC2, security groups, IAM roles)
2. Deploy Docker Compose stack in OWP AWS
3. Load STAC catalog metadata from S3 to OWP PostgreSQL instance
4. Validate S3 asset accessibility from OWP environment
5. Conduct user acceptance testing with OWP team
6. Launch for production use
7. Provide OWP team training and documentation
8. Monitor and optimize with OWP team feedback

### Documentation Requirements for OWP Team
- Technical documentation of whole architecture
- Repository containing key files and scripts for easy deployment
- STAC API endpoint documentation (STAC Browser & OWP-specific URLs)
- QGIS connection instructions for OWP users
- Operational runbook for common maintenance tasks
- Disaster recovery procedures for OWP deployment
- Monitoring and alerting playbook

---

## Recommendations

### Primary Recommendation

**Deploy using Option A (Dedicated EC2 with S3 storage):**

**Rationale:**
- Meets all OWP requirements (24/7 availability)
- 40% lower cost than Fargate alternative
- Simpler deployment reduces implementation risk
- Easier for OWP team to troubleshoot and maintain
- Clear upgrade path to Fargate if OWP requirements change

### Secondary Recommendations

1. **Start with S3 Standard storage:** Implement lifecycle policies after OWP usage patterns are established
2. **AMI backups:** Automate via AWS Backup service for consistent disaster recovery
3. **CloudWatch monitoring:** Set up basic alerting for CPU, memory, and disk usage
4. **Basic authentication:** Use nginx with htpasswd for simple access control initially
5. **Documentation priority:** Focus on operational procedures to ensure OWP team readiness
6. **Training sessions:** Conduct hands-on training with OWP users on deployment, architecture, QGIS and STAC Browser access

---

## References & Resources

- stac-fastapi-pgstac: https://github.com/stac-utils/stac-fastapi-pgstac
- STAC Browser: https://github.com/radiantearth/stac-browser
- GDAL Virtual File Systems: https://gdal.org/user/virtual_file_systems.html
- AWS S3 Best Practices: https://docs.aws.amazon.com/AmazonS3/latest/userguide/best-practices.html

---
