#!/bin/bash
set -e

# Install Scala using Coursier
echo "Installing Scala 2.12.18..."
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs
chmod +x cs
./cs setup -y --apps scala:2.12.18,sbt,scalafmt,scalafix,coursier
rm cs

# Ensure PATH is set correctly for the current session and future sessions
export PATH=$HOME/.local/share/coursier/bin:$PATH
echo 'export PATH=$HOME/.local/share/coursier/bin:$PATH' >> ~/.bashrc

# Print versions for verification
echo "Installed versions:"
echo "Python: $(python --version)"
echo "Java: $(java -version 2>&1 | head -n 1)"
scala -version
aws --version
echo "Spark: $(spark-submit --version 2>&1 | grep version)"

echo "Post-create script completed successfully."