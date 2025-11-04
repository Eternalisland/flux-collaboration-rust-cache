
import http from './http'

export async function fetchCusterRustJniStatus(baseUrls: string[]) {
    const tasks = baseUrls.map(async (base) => {
        try {
            const data = await fetchDatahubRustJniStatus(base)
            return { ok: true, baseUrl: base, ...data }
        } catch (e: any) {
            return { ok: false, baseUrl: base, error: e?.message || String(e) }
        }
    })
    return Promise.all(tasks)
}

import $ from 'jquery'

export async function fetchDatahubRustJniStatus(baseUrl?: string) {
    const root = baseUrl ? baseUrl.replace(/\/$/, '') : ''
    // Default: legacy gateway endpoint
    const url = `${root}/datahub01webApp/getJsonAction`
    let res: any
    if (typeof $ !== 'undefined' && $) {
        // 使用jQuery的$.ajax方式
        let urlParams = { clsid: 'd1510_header', method: 'getRustJniStatus' }
        res = await new Promise<{ status: number; data: any }>((resolve, reject) => {
            ;($ as any)
                .ajax({
                    type: 'POST',
                    url: url,
                    data: $.param(urlParams as any),
                    dataType: 'text',
                })
                .done(function (data: any) {
                    try {
                        // 尝试解析JSON响应
                        const parsedData = typeof data === 'string' ? JSON.parse(data) : data
                        resolve({
                            status: 200,
                            data: parsedData,
                        })
                    } catch (e) {
                        // 如果解析失败，返回原始数据
                        resolve({
                            status: 200,
                            data: { response: data, oK: true },
                        })
                    }
                })
                .fail(function (xhr: any, status: any, error: any) {
                    reject({
                        status: status || xhr.status || 500,
                        data: {
                            error: error || 'Request failed',
                            message: xhr.responseText || 'Unknown error',
                            oK: false,
                            errMsg: error || 'Request failed',
                        },
                    })
                })
        })
    } else {
        res = await http.post(url, null, {
            params: { clsid: 'd1510_header', method: 'getRustJniStatus' },
        })
    }
    const body = res.data

    // Accept multiple backend shapes
    const ok = !!(
        body &&
        typeof body === 'object' &&
        (body.result === 'OK' || body.ok === true || body.oK === true || body.state === true)
    )
    if (!ok) {
        const errMsg =
            (typeof body === 'string'
                ? body
                : body?.message || body?.errMsg || body?.error || body?.errorMsg || body?.extMsg) ||
            'Request failed'
        throw new Error(String(errMsg))
    }

    // Prefer body.data; some services return bizData (stringified JSON)
    let snapshot: any = (body as any).data
    if (!snapshot && (body as any).bizData != null) {
        snapshot = (body as any).bizData
    }
    if (typeof snapshot === 'string') {
        try {
            snapshot = JSON.parse(snapshot)
        } catch {
            /* ignore */
        }
    }
    if (!snapshot || typeof snapshot !== 'object') {
        throw new Error('Invalid scheduler snapshot payload')
    }

    return snapshot;
}
