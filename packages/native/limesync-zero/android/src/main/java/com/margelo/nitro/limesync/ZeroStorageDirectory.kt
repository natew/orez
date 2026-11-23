package com.margelo.nitro.limesync

import com.facebook.proguard.annotations.DoNotStrip
import com.margelo.nitro.NitroModules
import java.io.File

// the directory keeps One's name so caches written before the move still open
@DoNotStrip
object ZeroStorageDirectory {
    @JvmStatic
    @DoNotStrip
    fun path(): String? {
        val context = NitroModules.applicationContext ?: return null
        val directory = File(context.noBackupFilesDir, "one-zero")
        check(directory.isDirectory || directory.mkdirs()) { "Native Zero directory could not be created" }
        return directory.absolutePath
    }
}
