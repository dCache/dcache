// example test module for unit testing a worker

import { test, expect } from 'vitest'
import { handleMessage } from '../../workers/download.ts'

test('returns done for a valid request', () => {
    const result = handleMessage({ url: 'http://example.com/file', filename: 'file.txt' })
    expect(result).toEqual({ type: 'done' })
})
