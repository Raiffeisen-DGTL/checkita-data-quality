set -eu

NEW_DOC_VER="**Latest Version: $1**"
NEW_BUILD_VER="  val releaseVersion = \"$1\""
NEW_APP_VER="app-version = $1"

echo "Updating versions to $1 ..."

sed -i '/**Latest Version/c\'"$NEW_DOC_VER" docs/index.md
echo "Version is updated in file docs/index.md:"
head -n 3 docs/index.md | tail -n 1

sed -i '/**Latest Version/c\'"$NEW_DOC_VER" README.md
echo "Version is updated in file README.md:"
head -n 3 README.md | tail -n 1

sed -i '/  val releaseVersion/c\'"$NEW_BUILD_VER" project/Version.scala
echo "Version is updated in file project/Version.scala:"
head -n 2 project/Version.scala | tail -n 1

sed -i '/app-version/c\'"$NEW_APP_VER" checkita-core/src/main/resources/version-info.properties
echo "Version is updated in file checkita-core/src/main/resources/version-info.properties:"
head -n 1 checkita-core/src/main/resources/version-info.properties