set -eu

NEW_DOC_VER="**Latest Version: $1**"
NEW_DOC_VER_RU="**Актуальная версия: $1**"
NEW_BUILD_VER="  val releaseVersion = \"$1\""
NEW_APP_VER="app-version = $1"

echo "Updating versions to $1 ..."
sed -i '/**Latest Version/c\'"$NEW_DOC_VER" docs/en/index.md
sed -i '/**Актуальная версия/c\'"$NEW_DOC_VER_RU" docs/ru/index.md
sed -i '/**Latest Version/c\'"$NEW_DOC_VER" README.md
sed -i '/  val releaseVersion/c\'"$NEW_BUILD_VER" project/Version.scala
sed -i '/app-version/c\'"$NEW_APP_VER" checkita-core/src/main/resources/version-info.properties