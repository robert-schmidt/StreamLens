#!/bin/bash
#
# StreamLens Cross-Platform Packaging Script
# Creates native installers using jpackage:
#   - macOS: .app bundle and .dmg installer
#   - Windows: .exe application and .msi installer
#
# Note: jpackage can only create packages for the OS it's running on.
# Run this script on macOS for Mac builds, and on Windows (via Git Bash/WSL) for Windows builds.
#

set -e

APP_NAME="StreamLens"
APP_VERSION="1.0.0"
VENDOR="StreamLens"
MAIN_CLASS="org.springframework.boot.loader.launch.JarLauncher"

# Directories
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGET_DIR="${PROJECT_DIR}/target"
STAGING_DIR="${TARGET_DIR}/staging"
DIST_DIR="${TARGET_DIR}/dist"
ICON_DIR="${TARGET_DIR}/icons"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Darwin*)    echo "macos" ;;
        MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
        Linux*)
            # Check if running on WSL
            if grep -qEi "(Microsoft|WSL)" /proc/version 2>/dev/null; then
                echo "windows"
            else
                echo "linux"
            fi
            ;;
        *)          echo "unknown" ;;
    esac
}

OS_TYPE=$(detect_os)

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}StreamLens Packaging Script${NC}"
echo -e "${GREEN}Detected OS: ${OS_TYPE}${NC}"
echo -e "${GREEN}========================================${NC}"

# Step 1: Build JAR
echo -e "\n${YELLOW}Step 1: Building JAR...${NC}"
cd "${PROJECT_DIR}"

if [ "$OS_TYPE" = "windows" ]; then
    mvn.cmd clean package -DskipTests 2>/dev/null || mvn clean package -DskipTests
else
    mvn clean package -DskipTests
fi

JAR_FILE="${TARGET_DIR}/streamlens-${APP_VERSION}.jar"
if [ ! -f "${JAR_FILE}" ]; then
    echo -e "${RED}Error: JAR file not found: ${JAR_FILE}${NC}"
    exit 1
fi
echo -e "${GREEN}JAR built successfully: ${JAR_FILE}${NC}"

# Step 2: Create staging directory
echo -e "\n${YELLOW}Step 2: Preparing staging directory...${NC}"
rm -rf "${STAGING_DIR}" "${DIST_DIR}"
mkdir -p "${STAGING_DIR}" "${DIST_DIR}"
cp "${JAR_FILE}" "${STAGING_DIR}/"
echo -e "${GREEN}Staging directory prepared${NC}"

# Step 3: Create platform-specific icon
echo -e "\n${YELLOW}Step 3: Creating application icon...${NC}"
PNG_SOURCE="${PROJECT_DIR}/src/main/resources/icons/streamlens.png"
ICON_FILE=""

if [ -f "${PNG_SOURCE}" ]; then
    mkdir -p "${ICON_DIR}"

    if [ "$OS_TYPE" = "macos" ]; then
        # Create macOS .icns icon
        ICONSET_DIR="${ICON_DIR}/StreamLens.iconset"
        rm -rf "${ICONSET_DIR}"
        mkdir -p "${ICONSET_DIR}"

        # Generate icon sizes
        sips -z 16 16     "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_16x16.png" 2>/dev/null
        sips -z 32 32     "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_16x16@2x.png" 2>/dev/null
        sips -z 32 32     "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_32x32.png" 2>/dev/null
        sips -z 64 64     "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_32x32@2x.png" 2>/dev/null
        sips -z 128 128   "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_128x128.png" 2>/dev/null
        sips -z 256 256   "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_128x128@2x.png" 2>/dev/null
        sips -z 256 256   "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_256x256.png" 2>/dev/null
        sips -z 512 512   "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_256x256@2x.png" 2>/dev/null
        sips -z 512 512   "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_512x512.png" 2>/dev/null
        sips -z 1024 1024 "${PNG_SOURCE}" --out "${ICONSET_DIR}/icon_512x512@2x.png" 2>/dev/null

        # Create .icns file
        iconutil -c icns "${ICONSET_DIR}" -o "${ICON_DIR}/${APP_NAME}.icns"
        ICON_FILE="${ICON_DIR}/${APP_NAME}.icns"
        echo -e "${GREEN}macOS icon created: ${ICON_FILE}${NC}"

    elif [ "$OS_TYPE" = "windows" ]; then
        # Create Windows .ico icon using ImageMagick (if available)
        ICO_FILE="${ICON_DIR}/${APP_NAME}.ico"

        if command -v magick &> /dev/null; then
            # ImageMagick 7
            magick "${PNG_SOURCE}" -define icon:auto-resize=256,128,64,48,32,16 "${ICO_FILE}"
            ICON_FILE="${ICO_FILE}"
            echo -e "${GREEN}Windows icon created: ${ICON_FILE}${NC}"
        elif command -v convert &> /dev/null; then
            # ImageMagick 6
            convert "${PNG_SOURCE}" -define icon:auto-resize=256,128,64,48,32,16 "${ICO_FILE}"
            ICON_FILE="${ICO_FILE}"
            echo -e "${GREEN}Windows icon created: ${ICON_FILE}${NC}"
        else
            echo -e "${YELLOW}Warning: ImageMagick not found. Install it to create Windows icons.${NC}"
            echo -e "${YELLOW}  choco install imagemagick   (Windows)${NC}"
            echo -e "${YELLOW}Proceeding without custom icon${NC}"
        fi
    fi
else
    echo -e "${YELLOW}Warning: Icon source not found at ${PNG_SOURCE}${NC}"
    echo -e "${YELLOW}Proceeding without custom icon${NC}"
fi

# Common jpackage arguments
COMMON_ARGS=(
    --name "${APP_NAME}"
    --app-version "${APP_VERSION}"
    --vendor "${VENDOR}"
    --input "${STAGING_DIR}"
    --main-jar "streamlens-${APP_VERSION}.jar"
    --main-class "${MAIN_CLASS}"
    --dest "${DIST_DIR}"
    --java-options "-Xmx512m"
    --java-options "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

# Platform-specific packaging
if [ "$OS_TYPE" = "macos" ]; then
    # ==================== macOS Packaging ====================

    # Step 4: Create .app bundle
    echo -e "\n${YELLOW}Step 4: Creating macOS .app bundle...${NC}"

    JPACKAGE_APP_ARGS=(
        "${COMMON_ARGS[@]}"
        --type app-image
        --mac-package-name "${APP_NAME}"
        --java-options "-Dapple.awt.application.name=${APP_NAME}"
    )

    if [ -n "${ICON_FILE}" ] && [ -f "${ICON_FILE}" ]; then
        JPACKAGE_APP_ARGS+=(--icon "${ICON_FILE}")
    fi

    jpackage "${JPACKAGE_APP_ARGS[@]}"

    if [ ! -d "${DIST_DIR}/${APP_NAME}.app" ]; then
        echo -e "${RED}Error: Failed to create .app bundle${NC}"
        exit 1
    fi
    echo -e "${GREEN}.app bundle created: ${DIST_DIR}/${APP_NAME}.app${NC}"

    # Step 5: Create DMG
    echo -e "\n${YELLOW}Step 5: Creating DMG installer...${NC}"

    JPACKAGE_DMG_ARGS=(
        "${COMMON_ARGS[@]}"
        --type dmg
        --mac-package-name "${APP_NAME}"
        --java-options "-Dapple.awt.application.name=${APP_NAME}"
    )

    if [ -n "${ICON_FILE}" ] && [ -f "${ICON_FILE}" ]; then
        JPACKAGE_DMG_ARGS+=(--icon "${ICON_FILE}")
    fi

    jpackage "${JPACKAGE_DMG_ARGS[@]}"

    DMG_FILE="${DIST_DIR}/${APP_NAME}-${APP_VERSION}.dmg"
    if [ ! -f "${DMG_FILE}" ]; then
        echo -e "${RED}Error: Failed to create DMG${NC}"
        exit 1
    fi
    echo -e "${GREEN}DMG created: ${DMG_FILE}${NC}"

    # Summary for macOS
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}macOS Packaging Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "App bundle: ${DIST_DIR}/${APP_NAME}.app"
    echo -e "DMG installer: ${DMG_FILE}"
    echo -e ""
    echo -e "To install:"
    echo -e "  1. Open ${DMG_FILE}"
    echo -e "  2. Drag ${APP_NAME} to Applications"

    # Step 6: Create Windows .exe (cross-platform using Launch4j)
    echo -e "\n${YELLOW}Step 6: Creating Windows .exe (via Launch4j)...${NC}"

    # Check if Launch4j is installed
    LAUNCH4J=""
    if [ -d "/Applications/launch4j" ]; then
        LAUNCH4J="/Applications/launch4j/launch4j"
    elif [ -d "$HOME/Applications/launch4j" ]; then
        LAUNCH4J="$HOME/Applications/launch4j/launch4j"
    elif command -v launch4j &> /dev/null; then
        LAUNCH4J="launch4j"
    fi

    if [ -z "$LAUNCH4J" ]; then
        echo -e "${YELLOW}Launch4j not found. To create Windows .exe from macOS:${NC}"
        echo -e "${YELLOW}  1. Download from: https://launch4j.sourceforge.net/${NC}"
        echo -e "${YELLOW}  2. Extract to /Applications/launch4j/ or ~/Applications/launch4j/${NC}"
        echo -e "${YELLOW}  Or install via: brew install launch4j${NC}"
    else
        # Fix Windows line endings in launch4j script if needed
        sed -i '' 's/\r$//' "${LAUNCH4J}" 2>/dev/null || true
        chmod +x "${LAUNCH4J}" 2>/dev/null || true
        # Create Windows .ico icon if we have ImageMagick
        WIN_ICON=""
        if [ -f "${PNG_SOURCE}" ]; then
            WIN_ICO="${ICON_DIR}/${APP_NAME}.ico"
            if command -v magick &> /dev/null; then
                magick "${PNG_SOURCE}" -define icon:auto-resize=256,128,64,48,32,16 "${WIN_ICO}" 2>/dev/null && WIN_ICON="${WIN_ICO}"
                echo -e "${GREEN}Windows icon created: ${WIN_ICO}${NC}"
            elif command -v convert &> /dev/null; then
                convert "${PNG_SOURCE}" -define icon:auto-resize=256,128,64,48,32,16 "${WIN_ICO}" 2>/dev/null && WIN_ICON="${WIN_ICO}"
                echo -e "${GREEN}Windows icon created: ${WIN_ICO}${NC}"
            fi
        fi

        # Create Launch4j config XML
        L4J_CONFIG="${TARGET_DIR}/launch4j-config.xml"
        EXE_OUTPUT="${DIST_DIR}/${APP_NAME}.exe"

        cat > "${L4J_CONFIG}" << 'LAUNCH4J_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<launch4jConfig>
  <dontWrapJar>false</dontWrapJar>
  <headerType>gui</headerType>
  <jar>__JAR_FILE__</jar>
  <outfile>__EXE_OUTPUT__</outfile>
  <errTitle>StreamLens</errTitle>
  <cmdLine></cmdLine>
  <chdir>.</chdir>
  <priority>normal</priority>
  <downloadUrl>https://adoptium.net/</downloadUrl>
  <supportUrl></supportUrl>
  <stayAlive>false</stayAlive>
  <restartOnCrash>false</restartOnCrash>
  <manifest></manifest>
  __ICON_LINE__
  <classPath>
    <mainClass>org.springframework.boot.loader.launch.JarLauncher</mainClass>
  </classPath>
  <jre>
    <path></path>
    <bundledJre64Bit>false</bundledJre64Bit>
    <bundledJreAsFallback>false</bundledJreAsFallback>
    <minVersion>21</minVersion>
    <maxVersion></maxVersion>
    <jdkPreference>preferJre</jdkPreference>
    <runtimeBits>64</runtimeBits>
    <opt>-Xmx512m</opt>
    <opt>--add-opens=java.base/java.lang=ALL-UNNAMED</opt>
  </jre>
  <versionInfo>
    <fileVersion>1.0.0.0</fileVersion>
    <txtFileVersion>__APP_VERSION__</txtFileVersion>
    <fileDescription>StreamLens - Kafka &amp; Event Hubs Browser</fileDescription>
    <copyright>Copyright StreamLens</copyright>
    <productVersion>1.0.0.0</productVersion>
    <txtProductVersion>__APP_VERSION__</txtProductVersion>
    <productName>StreamLens</productName>
    <companyName>StreamLens</companyName>
    <internalName>StreamLens</internalName>
    <originalFilename>StreamLens.exe</originalFilename>
    <trademarks></trademarks>
    <language>ENGLISH_US</language>
  </versionInfo>
</launch4jConfig>
LAUNCH4J_EOF

        # Replace placeholders
        sed -i.bak "s|__JAR_FILE__|${JAR_FILE}|g" "${L4J_CONFIG}"
        sed -i.bak "s|__EXE_OUTPUT__|${EXE_OUTPUT}|g" "${L4J_CONFIG}"
        sed -i.bak "s|__APP_VERSION__|${APP_VERSION}|g" "${L4J_CONFIG}"
        if [ -n "$WIN_ICON" ]; then
            sed -i.bak "s|__ICON_LINE__|<icon>${WIN_ICON}</icon>|g" "${L4J_CONFIG}"
        else
            sed -i.bak "s|__ICON_LINE__||g" "${L4J_CONFIG}"
        fi
        rm -f "${L4J_CONFIG}.bak"

        # Run Launch4j
        if "${LAUNCH4J}" "${L4J_CONFIG}" 2>/dev/null; then
            echo -e "${GREEN}Windows .exe created: ${EXE_OUTPUT}${NC}"
            echo -e "${YELLOW}Note: This .exe requires Java 21+ installed on Windows${NC}"
            echo -e ""
            echo -e "Windows executable: ${EXE_OUTPUT}"
        else
            echo -e "${YELLOW}Launch4j failed. The .exe may not have been created.${NC}"
        fi
    fi

elif [ "$OS_TYPE" = "windows" ]; then
    # ==================== Windows Packaging ====================

    # Step 4: Create .exe application image
    echo -e "\n${YELLOW}Step 4: Creating Windows application...${NC}"

    JPACKAGE_EXE_ARGS=(
        "${COMMON_ARGS[@]}"
        --type app-image
        --win-console
    )

    if [ -n "${ICON_FILE}" ] && [ -f "${ICON_FILE}" ]; then
        JPACKAGE_EXE_ARGS+=(--icon "${ICON_FILE}")
    fi

    jpackage "${JPACKAGE_EXE_ARGS[@]}"

    if [ ! -d "${DIST_DIR}/${APP_NAME}" ]; then
        echo -e "${RED}Error: Failed to create Windows application${NC}"
        exit 1
    fi
    echo -e "${GREEN}Windows application created: ${DIST_DIR}/${APP_NAME}/${NC}"

    # Step 5: Create MSI installer
    echo -e "\n${YELLOW}Step 5: Creating MSI installer...${NC}"

    # Check for WiX Toolset (required for MSI on Windows)
    if command -v candle &> /dev/null || [ -d "/c/Program Files (x86)/WiX Toolset"* ] 2>/dev/null; then
        JPACKAGE_MSI_ARGS=(
            "${COMMON_ARGS[@]}"
            --type msi
            --win-dir-chooser
            --win-menu
            --win-shortcut
            --win-shortcut-prompt
        )

        if [ -n "${ICON_FILE}" ] && [ -f "${ICON_FILE}" ]; then
            JPACKAGE_MSI_ARGS+=(--icon "${ICON_FILE}")
        fi

        jpackage "${JPACKAGE_MSI_ARGS[@]}"

        MSI_FILE="${DIST_DIR}/${APP_NAME}-${APP_VERSION}.msi"
        if [ -f "${MSI_FILE}" ]; then
            echo -e "${GREEN}MSI installer created: ${MSI_FILE}${NC}"
        else
            echo -e "${YELLOW}Warning: MSI creation may have failed${NC}"
        fi
    else
        echo -e "${YELLOW}Warning: WiX Toolset not found. Skipping MSI creation.${NC}"
        echo -e "${YELLOW}Install WiX Toolset to create MSI installers:${NC}"
        echo -e "${YELLOW}  choco install wixtoolset${NC}"
    fi

    # Step 6: Create EXE installer (alternative to MSI)
    echo -e "\n${YELLOW}Step 6: Creating EXE installer...${NC}"

    JPACKAGE_EXE_INSTALLER_ARGS=(
        "${COMMON_ARGS[@]}"
        --type exe
        --win-dir-chooser
        --win-menu
        --win-shortcut
        --win-shortcut-prompt
    )

    if [ -n "${ICON_FILE}" ] && [ -f "${ICON_FILE}" ]; then
        JPACKAGE_EXE_INSTALLER_ARGS+=(--icon "${ICON_FILE}")
    fi

    jpackage "${JPACKAGE_EXE_INSTALLER_ARGS[@]}" 2>/dev/null || echo -e "${YELLOW}EXE installer creation skipped (requires Inno Setup or WiX)${NC}"

    EXE_INSTALLER="${DIST_DIR}/${APP_NAME}-${APP_VERSION}.exe"
    if [ -f "${EXE_INSTALLER}" ]; then
        echo -e "${GREEN}EXE installer created: ${EXE_INSTALLER}${NC}"
    fi

    # Summary for Windows
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}Windows Packaging Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "Application folder: ${DIST_DIR}/${APP_NAME}/"
    echo -e "  Run: ${DIST_DIR}/${APP_NAME}/${APP_NAME}.exe"
    [ -f "${DIST_DIR}/${APP_NAME}-${APP_VERSION}.msi" ] && echo -e "MSI installer: ${DIST_DIR}/${APP_NAME}-${APP_VERSION}.msi"
    [ -f "${DIST_DIR}/${APP_NAME}-${APP_VERSION}.exe" ] && echo -e "EXE installer: ${DIST_DIR}/${APP_NAME}-${APP_VERSION}.exe"
    echo -e ""
    echo -e "To install:"
    echo -e "  Option 1: Run the MSI/EXE installer"
    echo -e "  Option 2: Run ${APP_NAME}.exe directly from the application folder"

elif [ "$OS_TYPE" = "linux" ]; then
    # ==================== Linux Packaging ====================

    echo -e "\n${YELLOW}Step 4: Creating Linux application...${NC}"

    JPACKAGE_LINUX_ARGS=(
        "${COMMON_ARGS[@]}"
        --type app-image
    )

    if [ -f "${PNG_SOURCE}" ]; then
        JPACKAGE_LINUX_ARGS+=(--icon "${PNG_SOURCE}")
    fi

    jpackage "${JPACKAGE_LINUX_ARGS[@]}"

    echo -e "${GREEN}Linux application created: ${DIST_DIR}/${APP_NAME}/${NC}"

    # Create .deb package
    echo -e "\n${YELLOW}Step 5: Creating DEB package...${NC}"

    JPACKAGE_DEB_ARGS=(
        "${COMMON_ARGS[@]}"
        --type deb
        --linux-shortcut
    )

    if [ -f "${PNG_SOURCE}" ]; then
        JPACKAGE_DEB_ARGS+=(--icon "${PNG_SOURCE}")
    fi

    jpackage "${JPACKAGE_DEB_ARGS[@]}" 2>/dev/null || echo -e "${YELLOW}DEB creation skipped (requires dpkg)${NC}"

    # Summary for Linux
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}Linux Packaging Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "Application: ${DIST_DIR}/${APP_NAME}/"
    [ -f "${DIST_DIR}/${APP_NAME}-${APP_VERSION}.deb" ] && echo -e "DEB package: ${DIST_DIR}/${APP_NAME}-${APP_VERSION}.deb"

else
    echo -e "${RED}Error: Unsupported operating system: ${OS_TYPE}${NC}"
    echo -e "${YELLOW}This script supports: macOS, Windows (via Git Bash/WSL), Linux${NC}"
    exit 1
fi

echo -e ""
