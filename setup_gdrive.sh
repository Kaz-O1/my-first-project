#!/bin/bash
# One-time setup: auto-mount Google Drive G: in WSL via fstab + systemd
set -e

GDRIVE_ROOT="/mnt/g/My Drive/חשבוניות אוהד כזום"

echo "=== Google Drive WSL Setup ==="

# 1. Remove broken sudoers file if it exists
if [ -f /etc/sudoers.d/gdrive-mount ]; then
    sudo rm /etc/sudoers.d/gdrive-mount
    echo "✓ Removed broken sudoers entry"
fi

# 2. Create mount point
sudo mkdir -p /mnt/g
echo "✓ Created /mnt/g"

# 3. Add to /etc/fstab for automatic mounting (systemd handles this on WSL startup)
if ! grep -q "drvfs.*G:" /etc/fstab 2>/dev/null; then
    echo "G: /mnt/g drvfs defaults 0 0" | sudo tee -a /etc/fstab > /dev/null
    echo "✓ Added G: to /etc/fstab (will auto-mount on WSL startup)"
else
    echo "✓ /etc/fstab already configured"
fi

# 4. Mount now (without needing it in sudoers — just once with password)
sudo mount /mnt/g 2>/dev/null || sudo mount -t drvfs G: /mnt/g 2>/dev/null || true

# 5. Test access
if ls "/mnt/g/My Drive/" > /dev/null 2>&1; then
    echo "✓ Google Drive accessible at /mnt/g/"
else
    echo "✗ Mount failed. Make sure Google Drive for Desktop is running on Windows."
    exit 1
fi

# 6. Create folder structure in Google Drive
mkdir -p "$GDRIVE_ROOT/ALL Invoices/ohad"
mkdir -p "$GDRIVE_ROOT/ALL Invoices/cril-tech"
mkdir -p "$GDRIVE_ROOT/Reports"
echo "✓ Folder structure ready:"
echo "    G:\\My Drive\\חשבוניות אוהד כזום\\ALL Invoices\\ohad\\"
echo "    G:\\My Drive\\חשבוניות אוהד כזום\\ALL Invoices\\cril-tech\\"
echo "    G:\\My Drive\\חשבוניות אוהד כזום\\Reports\\"

echo ""
echo "=== Setup complete! ==="
echo "From now on G: will mount automatically when WSL starts."
echo "You can now run Invoice Processor.bat"
