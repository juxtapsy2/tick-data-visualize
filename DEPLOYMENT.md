# Deployment Guide: Railway (Database) + Render (Services)

This guide walks you through deploying TimescaleDB on Railway and your application services on Render.

## 📋 Overview

```
Railway (Database)          Render (Application)
┌─────────────────┐         ┌──────────────────┐
│                 │         │                  │
│  TimescaleDB    │◄────────┤  Backend Service │
│  PostgreSQL     │  SSL    │  Data Generator  │
│  Port: 5432     │         │  Frontend        │
│                 │         │  Redis           │
└─────────────────┘         └──────────────────┘
```

## Part 1: Deploy TimescaleDB on Railway

### Step 1: Create New PostgreSQL Database

1. Go to [Railway.app](https://railway.app)
2. Click **"New Project"**
3. Select **"Provision PostgreSQL"**
4. Wait for deployment to complete

### Step 2: Enable TimescaleDB Extension

1. In Railway, click on your PostgreSQL service
2. Click **"Connect"** tab
3. Copy the connection string (starts with `postgresql://`)
4. Connect using psql or a database client:

```bash
# Using Railway's connection URL
psql "postgresql://postgres:password@host.railway.app:port/railway"

# Or use Railway CLI
railway connect postgres
```

5. Enable TimescaleDB:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

### Step 3: Initialize Database Schema

**Option A: Using Railway CLI**
```bash
# Install Railway CLI if needed
npm i -g @railway/cli

# Link to your project
railway link

# Connect and run init scripts
railway connect postgres < timescaledb_host/init/01-init-extensions.sql
railway connect postgres < timescaledb_host/init/02-create-schemas.sql
railway connect postgres < timescaledb_host/init/03-create-tables.sql
railway connect postgres < timescaledb_host/init/04-create-hypertables.sql
railway connect postgres < timescaledb_host/init/05-create-continuous-aggregates.sql
railway connect postgres < timescaledb_host/init/06-create-functions.sql
railway connect postgres < timescaledb_host/init/07-create-notify-triggers.sql
railway connect postgres < timescaledb_host/init/08-seed-metadata.sql
```

**Option B: Manual Connection**
```bash
# Get connection details from Railway dashboard
psql -h your-host.railway.app -U postgres -d railway

# Then copy and paste SQL from init scripts
\i /path/to/timescaledb_host/init/01-init-extensions.sql
# ... etc
```

**Option C: Deploy timescaledb_host Docker to Railway**
```bash
cd timescaledb_host

# Deploy to Railway
railway init
railway up
```

### Step 4: Get Connection Details

From Railway dashboard, note these values:
- **Host**: `your-instance.railway.app`
- **Port**: `5432` (or custom)
- **Database**: `railway`
- **User**: `postgres`
- **Password**: (shown in dashboard)

## Part 2: Deploy Application Services on Render

### Step 1: Prepare Repository

```bash
cd /Users/bill/Desktop/qubit/data-visualize

# Make sure everything is committed
git add .
git commit -m "Add Render deployment configuration"
git push origin main
```

### Step 2: Deploy on Render

1. Go to [render.com](https://render.com)
2. Click **"New" → "Blueprint"**
3. Connect your GitHub repository
4. Render will detect `render.yaml`
5. **BEFORE** clicking "Apply":

### Step 3: Configure Railway Database Connection

In Render dashboard, add these environment variables to both `market-service` and `data-generator`:

```
MARKET_DATABASE_HOST=your-instance.railway.app
MARKET_DATABASE_PORT=5432
MARKET_DATABASE_USER=postgres
MARKET_DATABASE_PASSWORD=<your-railway-password>
MARKET_DATABASE_DATABASE=railway
```

**Important:** Mark `MARKET_DATABASE_PASSWORD` as a **secret** (encrypted).

### Step 4: Deploy

1. Click **"Apply"** to start deployment
2. Services will deploy in order:
   - Redis (managed)
   - Backend Service
   - Data Generator
   - Frontend

### Step 5: Monitor Deployment

```bash
# Check logs in Render dashboard
# Or use Render CLI
render deploy logs market-service
render deploy logs data-generator
```

## Part 3: Verify Connection

### Test Database Connection

```bash
# From Render service shell (use dashboard)
wget -qO- http://localhost:8080/health
```

Expected response:
```json
{
  "status": "healthy",
  "checks": {
    "postgres": "ok",
    "redis": "ok"
  }
}
```

### Test WebSocket Connection

```bash
# Use wscat or browser console
wscat -c wss://market-service.onrender.com/ws
```

### Test Frontend

Open: `https://market-frontend.onrender.com`

## 🔧 Configuration Reference

### Railway Environment Variables

```bash
# Railway auto-generates these
PGHOST=your-instance.railway.app
PGPORT=5432
PGUSER=postgres
PGPASSWORD=<generated>
PGDATABASE=railway
DATABASE_URL=postgresql://postgres:pass@host:port/railway
```

### Render Environment Variables

Copy from Railway to Render:

| Render Variable Name           | Railway Source          | Required |
|--------------------------------|-------------------------|----------|
| MARKET_DATABASE_HOST           | PGHOST                  | Yes      |
| MARKET_DATABASE_PORT           | PGPORT                  | Yes      |
| MARKET_DATABASE_USER           | PGUSER                  | Yes      |
| MARKET_DATABASE_PASSWORD       | PGPASSWORD (secret)     | Yes      |
| MARKET_DATABASE_DATABASE       | PGDATABASE              | Yes      |

## 🐛 Troubleshooting

### Can't Connect to Railway Database

**Issue:** Connection timeout or refused

**Solutions:**
1. Check Railway firewall - add Render IPs if restricted
2. Verify SSL mode is set to `require`
3. Test connection from Render shell:

```bash
# In Render shell
nc -zv your-instance.railway.app 5432
```

### TimescaleDB Extension Not Found

**Issue:** `ERROR: extension "timescaledb" does not exist`

**Solution:**
```sql
-- Connect to Railway database
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

### Data Generator Not Running

**Issue:** Worker service crashes immediately

**Solutions:**
1. Check CSV files exist in `services/data-generator/data/`
2. Verify database schema is initialized
3. Check logs in Render dashboard

### WebSocket Connection Failed

**Issue:** Frontend can't connect to WebSocket

**Solutions:**
1. Verify backend is running: `https://market-service.onrender.com/health`
2. Check CORS settings in backend
3. Update frontend env vars with correct WebSocket URL

## 📊 Cost Estimate

### Railway
- **Starter Plan**: $5/month (includes 500 hours, ~$0.01/hour after)
- **PostgreSQL**: ~$5-10/month depending on usage

### Render
- **Starter Plan**: $7/month per service
- **Redis**: Free tier or $7/month
- **Total**: ~$21-28/month (3 services + Redis)

**Total Combined**: ~$26-38/month

## 🔒 Security Best Practices

1. **Use Environment Variables** - Never hardcode credentials
2. **Enable SSL** - Set `MARKET_DATABASE_SSL_MODE=require`
3. **Restrict Access** - Configure Railway firewall rules
4. **Rotate Passwords** - Change Railway password periodically
5. **Monitor Logs** - Check for unauthorized access attempts

## 🚀 Deployment Checklist

- [ ] Railway PostgreSQL created
- [ ] TimescaleDB extension enabled
- [ ] Database schema initialized (all init scripts)
- [ ] Connection details saved securely
- [ ] Repository pushed to GitHub
- [ ] Render blueprint created
- [ ] Environment variables configured
- [ ] Services deployed successfully
- [ ] Health checks passing
- [ ] WebSocket connection working
- [ ] Frontend accessible

## 📝 Useful Commands

### Railway CLI
```bash
# Install
npm i -g @railway/cli

# Link project
railway link

# Connect to database
railway connect postgres

# View logs
railway logs

# Run migrations
railway run psql < migration.sql
```

### Render CLI
```bash
# Install
brew install render

# Login
render auth login

# View services
render services list

# View logs
render logs market-service

# SSH into service
render shell market-service
```

## 🔄 Updates and Redeployments

### Update Application Code
```bash
git add .
git commit -m "Update feature"
git push
```

Render will auto-deploy on push.

### Update Database Schema
```bash
# Connect to Railway
railway connect postgres

# Run migration
\i new-migration.sql
```

### Force Redeploy on Render
```bash
render deploy --service market-service
```

## 📈 Monitoring

### Check Health Endpoints

```bash
# Backend health
curl https://market-service.onrender.com/health

# Backend metrics
curl https://market-service.onrender.com/metrics

# Frontend health
curl https://market-frontend.onrender.com/health
```

### Database Stats

```sql
-- Connection count
SELECT count(*) FROM pg_stat_activity WHERE datname = 'railway';

-- Database size
SELECT pg_size_pretty(pg_database_size('railway'));

-- Table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname IN ('market', 'aggregates')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

---

**Ready to deploy?** Start with Part 1 above! 🚀
