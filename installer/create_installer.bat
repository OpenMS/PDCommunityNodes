REM harvest all files from share/OpenMS folder and automatically generate components, GUIDs so we don't need to do this manually for hundreds of files.
"C:\Program Files (x86)\WiX Toolset v3.11\bin\heat" dir "SourceDir\share\OpenMS" -gg -sfrag -sreg -template component -cg OpenMSShareComponents -dr NuXLShareOpenMS -o "OpenMSShareComponents.wxs"
REM compile to wixobj
"C:\Program Files (x86)\WiX Toolset v3.11\bin\candle" "OpenMSShareComponents.wxs" "NuXLMain.wxs"
REM link into msi installer
"C:\Program Files (x86)\WiX Toolset v3.11\bin\light" -o PDNuXLNodes.wsi NuXLMain.wixobj OpenMSShareComponents.wixobj