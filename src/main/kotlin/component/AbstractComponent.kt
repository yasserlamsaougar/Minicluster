package component

import com.github.salomonbrys.kodein.KodeinInjector

abstract class AbstractComponent<out T> : Component<T> {
    override val injector: KodeinInjector = KodeinInjector()
}