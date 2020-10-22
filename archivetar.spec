# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
import globus_sdk
globus_path = Path(globus_sdk.__file__).parent
block_cipher = None


a = Analysis(['bin/archivetar'],
             pathex=['/home/brockp/archivetar'],
             binaries=[],
             datas=[(globus_path,"globus_sdk")],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='archivetar',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True )
