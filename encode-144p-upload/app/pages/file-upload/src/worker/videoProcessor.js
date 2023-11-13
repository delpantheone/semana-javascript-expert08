export default class VideoProcessor {
    #mp4Demuxer
    #webMWriter
    #service
    #buffers = []
    /**
     * 
     * @param {object} options
     * @param {import('./mp4Demuxer.js').default} options.mp4Demuxer
     * @param {import('../deps/webm-writer2.js').default} options.webMWriter
     * @param {import('./service.js').default} options.service
     */
    constructor({ mp4Demuxer, webMWriter, service }) {
        this.#mp4Demuxer = mp4Demuxer
        this.#webMWriter = webMWriter
        this.#service = service
    }
    /** @returns {ReadableStream} */
    mp4Decoder(stream) {

        return new ReadableStream({
            start: async (controller) => {
                const decoder = new VideoDecoder({
                    /** @param {VideoFrame} frame */
                    output(frame) {
                        controller.enqueue(frame)
                    },
                    error(e) {
                        console.error('error at mp4Decoder', e)
                        controller.error(e)
                    }
                })

                return this.#mp4Demuxer.run(stream, {
                    onConfig(config) {
                        decoder.configure(config)
                    },
                    /** @param {EncodedVideoChunk} chunk */
                    onChunk(chunk) {
                        decoder.decode(chunk)
                    }
                })
            }
        })

    }

    encode144p(encoderConfig) {
        let _encoder
        const readable = new ReadableStream({
            start: async (controller) => {
                const { supported } = await VideoEncoder.isConfigSupported(encoderConfig)
                if (!supported) {
                    const message = 'encode144p VideoDecoder not supported'
                    console.error(message, encoderConfig)
                    controller.error(message)
                    return
                }
                _encoder = new VideoEncoder({
                    /**
                     * 
                     * @param {EncodedVideoChunk} frame 
                     * @param {EncodedVideoChunkMetadata} config 
                     */
                    output: (frame, config) => {
                        if (config.decoderConfig) {
                            const decoderConfig = {
                                type: 'config',
                                config: config.decoderConfig
                            }
                            controller.enqueue(decoderConfig)
                        }
                        controller.enqueue(frame)
                    },
                    error: (err) => {
                        console.error('VideoEncoder 144p', err)
                        controller.error(err)
                    }
                })

                await _encoder.configure(encoderConfig)
            }
        })
        const writable = new WritableStream({
            async write(frame) {
                _encoder.encode(frame)
                frame.close()
            }
        })
        return {
            readable,
            writable
        }
    }

    renderDecodedFramesAndGetEncodedChunks(renderFrame) {
        let _decoder
        return new TransformStream({
            start(controller) {
                _decoder = new VideoDecoder({
                    output(frame) {
                        renderFrame(frame)
                    },
                    error(err) {
                        console.error('error at renderFrames', err)
                        controller.error(err)
                    }
                })
            },
            /**
             * 
             * @param {EncodedVideoChunk} encodedChunk 
             * @param {TransformStreamDefaultController} controller 
             */
            async transform(encodedChunk, controller) {
                if (encodedChunk.type === 'config') {
                    await _decoder.configure(encodedChunk.config)
                    return
                }
                _decoder.decode(encodedChunk)
                //need encoded version to use webM
                controller.enqueue(encodedChunk)
            }
        })
    }
    transformIntoWebM() {
        const writable = new WritableStream({
            write: (chunk) => {
                this.#webMWriter.addFrame(chunk)
            },
            close() {

            }
        })
        return {
            readable: this.#webMWriter.getStream(),
            writable
        }
    }

    upload(fileName, resolution, type) {
        const chunks = []
        let byteCount = 0
        let segmentCount = 0
        const triggerUpload = async chunks => {
            const blob = new Blob(chunks, { type: 'video/webm' })
            await this.#service.uploadFile({
                fileName: `${fileName}-${resolution}.${++segmentCount}.${type}`,
                fileBuffer: blob
            })
            chunks.length = 0 // Gambiarra para limpar os chunks da memória
            byteCount = 0
        }
        return new WritableStream({
            /**
            * 
            * @param {object} options
            * @param {Uint8Array} options.data
            */
            async write({ data }) {
                chunks.push(data)
                byteCount += data.byteLength
                if (byteCount <= 10e6) return
                await triggerUpload(chunks)
            },
            async close() {
                if (!chunks.length) return
                await triggerUpload(chunks)
            }
        })
    }

    async start({ file, encoderConfig, renderFrame, sendMessage }) {
        const stream = file.stream()
        const fileName = file.name.split('/').pop().replace('.mp4', '')
        await this.mp4Decoder(stream)
            .pipeThrough(this.encode144p(encoderConfig))
            .pipeThrough(this.renderDecodedFramesAndGetEncodedChunks(renderFrame))
            .pipeThrough(this.transformIntoWebM())
            // .pipeThrough(
            //     new TransformStream({
            //         transform: ({ data, position }, controller) => {
            //             this.#buffers.push(data)
            //             controller.enqueue(data)
            //         },
            //         flush: () => {
            //             sendMessage({
            //                 status: 'done',
            //                 // buffers: this.#buffers,
            //                 // fileName: fileName.concat('-144p.webm')
            //             })
            //         }
            //     })
            // )
            .pipeTo(this.upload(fileName, '144p', 'webm'))
        sendMessage({
            status: 'done'
        })
        // .pipeTo(new WritableStream({
        //     write(frame) {
        //         // renderFrame(frame)
        //     }
        // }))
    }
}