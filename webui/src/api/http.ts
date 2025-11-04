import axios from 'axios'

const http = axios.create({
    // baseURL can be configured via env if needed
    timeout: 0,
    headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json'
    }
})

http.interceptors.request.use(
    (config) => {
        return config
    },
    (error) => Promise.reject(error)
)

http.interceptors.response.use(
    (response) => response,
    (error) => {
        const data = error?.response?.data
        const msg = (data && (data.message || data.errMsg || data.error || data.errorMsg || data.extMsg))
            || error?.message
            || 'Network Error'
        return Promise.reject(new Error(String(msg)))
    }
)

export default http
