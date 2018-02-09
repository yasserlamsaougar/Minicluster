package hooks

import com.github.salomonbrys.kodein.Kodein

interface Hook {

    fun before(kodein: Kodein) {

    }

    fun after(kodein: Kodein) {

    }
}