set -eu

echo "Updating changelog headers' levels ..."
sed -rie 's/^# \[/## \[/g' docs/changelog/CHANGELOG.md
