import { createApp } from 'vue';
import App from './App.vue';
export function mount(el: string | HTMLElement, language?: string) {
    const app = createApp(App)
    app.mount(el)
    return app
}

// 开发环境自动挂载，生产环境可以按需调用 mount
if (import.meta.env.DEV) {
    mount('#app')
}
