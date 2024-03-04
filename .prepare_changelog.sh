set -eu

echo "Updating changelog headers' levels ..."
sed -ri 's/^# \[/## \[/g' docs/changelog/CHANGELOG.md
echo "Headers' levels updated."