// Package localmq 提供直接寫入 shared durable storage 的 brokerless burst
// buffer。Package 只負責 WAL、delivery、checkpoint、retention 與 lifecycle；
// downstream business side effect 及其 idempotency 由使用端負責。
package localmq
