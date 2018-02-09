package hooks

import com.github.salomonbrys.kodein.Kodein

class InfoHook : Hook {
    override fun before(kodein: Kodein) {
        println("Will Start the Miniclusters after running hooks")
    }

    override fun after(kodein: Kodein) {
        println("Started all the Miniclusters now running hooks")
    }
}