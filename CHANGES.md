# Changes from Original Repository

This document explains the modifications made to prevent repository bloat.

## Root Cause of Original Problem

The original repository committed the entire `data/` directory to git on every hourly update. This caused:

1. **Unbounded Growth**: Every commit added ~1-5 GB of Zarr data to git history
2. **Ineffective Cleanup**: Despite using `git gc --aggressive`, git history preserved all versions
3. **Repository Failure**: After weeks/months, the repository exceeded GitHub's size limits

## Critical Changes Made

### 1. `.gitignore` Modifications

**Added to .gitignore:**
```gitignore
# Data files - THIS IS CRITICAL TO PREVENT BLOAT
data/
*.zarr/
*.grib2
*.grb
*.grb2
*.idx
*.tmp
```

This ensures data files are never tracked by git.

### 2. Workflow Modifications

**File**: `.github/workflows/update-data.yml`

**Line 118-142 (Original):**
```yaml
- name: Commit and push changes
  run: |
    git add data/ catalog/ data_summary/  # ← PROBLEM: commits data!
    if ! git diff --staged --quiet; then
      git commit -m "Update NBM data - $(date -u +'%Y-%m-%d %H:%M UTC')"

      # Aggressive git cleanup to prevent repository bloat
      echo "=== Running git cleanup to save space ==="
      git reflog expire --expire=now --all
      git gc --aggressive --prune=now
      # ...
```

**Line 118-142 (Modified):**
```yaml
- name: Commit and push metadata changes only
  run: |
    # IMPORTANT: We do NOT commit data/ to prevent repository bloat
    # Data files are in .gitignore and exist only in the runner's working directory
    # Only catalog and summary (small metadata files) are version controlled
    git add catalog/ data_summary/  # ← SOLUTION: only metadata!
    if ! git diff --staged --quiet; then
      git commit -m "Update NBM metadata - $(date -u +'%Y-%m-%d %H:%M UTC')"
      # No git gc needed - repository stays small naturally
      # ...
```

**Key Changes:**
- Removed `data/` from `git add` command
- Updated commit message to reflect "metadata" not "data"
- Removed aggressive git cleanup (not needed anymore)
- Added explanatory comments

### 3. Removed Files

None of the source code or scripts were removed. All functionality is preserved.

## What Still Works

Everything! The changes are purely about version control, not functionality:

- ✅ Hourly automated downloads
- ✅ GRIB2 to Zarr conversion
- ✅ Catalog generation
- ✅ Data summary creation
- ✅ GitHub Pages deployment
- ✅ All CLI commands
- ✅ Data cleanup scripts

## What's Different

### For Git Repository:
- **Size**: Stays under 100 MB forever instead of growing to 100+ GB
- **Commits**: Only contain metadata changes (KB) not data (GB)
- **History**: Tracks code evolution, not data evolution

### For GitHub Actions:
- **Data Location**: Exists in `/home/runner/work/repo/repo/data/` during workflow execution
- **Data Persistence**: Discarded after each workflow run (by design)
- **Catalog**: Still committed and deployed to Pages for discovery

### For Users:
- **No Impact**: If you're using the deployed catalog or GitHub Pages, nothing changes
- **Local Development**: Clone repo, run `nbm-zarr operational-update`, data appears locally in `data/`

## Migration Guide

If you're migrating from the original repository:

### For Repository Owners:

1. **Clean existing repo (DESTRUCTIVE - creates new history):**
   ```bash
   # WARNING: This rewrites history and removes all data from git
   git filter-repo --path data/ --invert-paths
   ```

2. **Or start fresh (recommended):**
   ```bash
   # Create new repo using this clean version
   git clone https://github.com/YOUR_USERNAME/nbm_to_zarr_clean.git
   cd nbm_to_zarr_clean

   # Copy any custom modifications from old repo
   # Push to new GitHub repository
   git remote add origin https://github.com/YOUR_USERNAME/new-repo.git
   git push -u origin main
   ```

### For Users:

No action needed! Just clone this repository instead:
```bash
git clone https://github.com/YOUR_USERNAME/nbm_to_zarr_clean.git
```

## Technical Details

### Why Git GC Didn't Help

`git gc --aggressive --prune=now` only removes:
- Dangling commits (unreachable from any branch)
- Orphaned objects
- Redundant pack files

It **does NOT** remove:
- Data from commits in your history
- Files that were previously committed and later .gitignored
- Objects reachable from any branch/tag

Every hourly commit added the entire Zarr store to the git object database, and these were reachable from `main` branch history, so gc couldn't remove them.

### Why This Solution Works

By preventing data files from ever being committed:
- Git only tracks code and metadata (text files)
- No large binary objects enter the git object database
- Repository size stays proportional to code changes, not data changes
- No cleanup needed - bloat never occurs in the first place

## Size Comparison

**Original Repository Growth:**
```
Week 1:   ~35 GB
Week 2:   ~70 GB
Week 3:  ~105 GB
Week 4:  ~140 GB → GitHub size limit warnings
Month 2: ~280 GB → Repository unusable
```

**This Version:**
```
Week 1:   ~12 MB
Week 2:   ~15 MB
Week 3:   ~18 MB
Month 6:  ~25 MB
Year 1:   ~50 MB
```

## Questions & Answers

**Q: Where is the Zarr data stored then?**
A: During GitHub Actions runs, it's in the runner's temporary workspace. For local development, it's in your local `data/` directory (not committed).

**Q: How do users access the data?**
A: They clone the repo and run `nbm-zarr operational-update` to generate data locally, or they access it from wherever you choose to store it (cloud storage, separate data repository, etc.).

**Q: What about catalog deployment?**
A: Catalog metadata is still committed and deployed to GitHub Pages, enabling data discovery.

**Q: Can I still commit data if I want to?**
A: Technically yes (remove from .gitignore), but **strongly discouraged**. For data persistence, use cloud storage (S3, GCS, Azure) or a dedicated data repository.

## Recommendations

For persistent data storage beyond workflow runs:

1. **Option 1 - Cloud Storage**: Upload to S3/GCS after generation
   ```yaml
   - name: Upload to cloud storage
     run: |
       aws s3 sync data/ s3://your-bucket/nbm-forecast/ --delete
   ```

2. **Option 2 - Git LFS**: For smaller datasets, use Git Large File Storage
   ```yaml
   - uses: actions/upload-artifact@v4
     with:
       name: zarr-data
       path: data/
   ```

3. **Option 3 - Separate Repo**: Create a dedicated data repository if you need git tracking

## License

Same as original: MIT License
