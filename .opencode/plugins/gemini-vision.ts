import { type Plugin, tool } from "@opencode-ai/plugin"
import { readFileSync, unlinkSync } from "node:fs"
import { extname, resolve } from "node:path"
import { execFileSync } from "node:child_process"

const IMAGE_EXTS: Record<string, string> = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".gif": "image/gif",
  ".bmp": "image/bmp",
}

function imageAsInline(
  value: string
): { mimeType: string; data: string } {
  if (value.startsWith("data:")) {
    const m = value.match(/^data:([^;,]+);base64,(.+)$/s)
    if (m) return { mimeType: m[1], data: m[2] }
  }
  const buf = readFileSync(resolve(value))
  const mime = IMAGE_EXTS[extname(value).toLowerCase()] ?? "image/png"
  return { mimeType: mime, data: buf.toString("base64") }
}

async function geminiRequest(
  parts: unknown[],
  apiKey: string,
  model: string,
  instruction: string
): Promise<string> {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    model
  )}:generateContent`
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-goog-api-key": apiKey,
    },
    body: JSON.stringify({
      contents: [{ role: "user", parts: [{ text: instruction }, ...parts] }],
      generationConfig: { maxOutputTokens: 2048 },
    }),
    signal: AbortSignal.timeout(120000),
  })

  const status = resp.status
  const body = (await resp.text()).trim()
  if (!resp.ok) {
    if (status === 400 && /API_KEY|key is not/i.test(body)) {
      return `Gemini Key 无效：${body.slice(0, 400)}\n请检查 GEMINI_API_KEY 是否正确。`
    }
    return `Gemini 识别失败（HTTP ${status}）：${body.slice(0, 500)}`
  }

  const data = JSON.parse(body)
  const text = data?.candidates?.[0]?.content?.parts
    ?.map((p: { text?: string }) => p.text ?? "")
    .join("")
  if (!text?.trim()) {
    const msg = data?.promptFeedback?.blockReason ??
      data?.candidates?.[0]?.finishReason ??
      "空响应"
    return `Gemini 未返回文字（${msg}）：${body.slice(0, 300)}`
  }
  return text
}

async function capturePage(
  url: string,
  width: number,
  height: number,
  fullPage: boolean
): Promise<string> {
  const tmp = `/tmp/opencode-page-${Date.now()}.png`
  try {
    const args = [
      "screenshot",
      "--browser",
      "chromium",
      "--viewport-size",
      `${width},${height}`,
    ]
    if (fullPage) args.push("--full-page")
    args.push(url, tmp)
    execFileSync("playwright", args, { stdio: "pipe" })
    return tmp
  } catch (err) {
    throw new Error(
      `页面截图失败：${err instanceof Error ? err.message : String(err)}。` +
        "请确认已安装 playwright（brew install playwright && playwright install chromium）。"
    )
  }
}

export const GeminiVisionPlugin: Plugin = async ({ }) => {
  return {
    tool: {
      inspect_page: tool({
        description:
          "用无头浏览器对指定网页截图，并调用 Gemini 视觉模型评估其实际渲染效果，返回文字报告。用于开发网页后自动验证视觉效果是否符合设计预期、有无布局错乱/元素缺失/样式问题。",
        args: {
          url: tool.schema.string().describe(
            "要检查的页面：http(s) URL，或本地 HTML 文件路径（会自动转 file://）"
          ),
          goals: tool.schema.string().optional().describe(
            "设计预期/要重点核对的内容，例如'左侧导航栏、顶部搜索框、卡片三列布局，主色 #4F46E5'。不填则只做通用视觉检查。"
          ),
          width: tool.schema.number().optional().describe(
            "截图视口宽度（px），默认 1440（桌面）或 390（移动端）"
          ),
          height: tool.schema.number().optional().describe(
            "截图视口高度（px），默认 900（桌面）或 844（移动端）"
          ),
          device: tool.schema.string().optional().describe(
            "视口预设：desktop（1440×900）或 mobile（390×844）。指定后若不传 width/height 则用预设值，传了则优先用你的值。默认 desktop"
          ),
          fullPage: tool.schema.boolean().optional().describe(
            "是否整页滚动长图截图（true 时截取页面完整高度，适合长页面验收）。默认 false 只截首屏"
          ),
        },
        async execute(args) {
          const apiKey = process.env.GEMINI_API_KEY
          if (!apiKey) {
            return "未配置 GEMINI_API_KEY 环境变量。请到 https://aistudio.google.com/apikey 申请免费 key，然后 export GEMINI_API_KEY=xxx 并重启 opencode。"
          }

          const model =
            process.env.GEMINI_VISION_MODEL ?? "gemini-flash-latest"
          const target =
            args.url.startsWith("file:") || /^https?:\/\//i.test(args.url)
              ? args.url
              : "file://" + resolve(args.url)
          const isMobile = (args.device ?? "desktop") === "mobile"
          const width = args.width ?? (isMobile ? 390 : 1440)
          const height = args.height ?? (isMobile ? 844 : 900)
          const fullPage = args.fullPage ?? false

          const shot = await capturePage(target, width, height, fullPage)
          try {
            const inline = imageAsInline(shot)
            const goals =
              args.goals?.trim() ??
              "检查页面整体布局、视觉设计是否正常，有无明显错乱、元素重叠、文字溢出或图片缺失。"
            const viewportNote = fullPage
              ? `（整页长图，设备：${isMobile ? "移动端" : "桌面端"} ${width}×${height}）`
              : `（首屏截图，设备：${isMobile ? "移动端" : "桌面端"} ${width}×${height}）`
            return await geminiRequest(
              [{ inlineData: inline }],
              apiKey,
              model,
              `这是对页面 ${target} ${viewportNote}。请以视觉验收工程师的视角检查它：\n${goals}\n\n请给出：\n1. 整体观感（布局/配色/层次）\n2. 与预期目标的差距清单（逐条，可执行）\n3. 明显缺陷（元素缺失、重叠、溢出、空图、文字截断等）`
            )
          } finally {
            try {
              unlinkSync(shot)
            } catch {}
          }
        },
      }),
      gemini_vision: tool({
        description:
          "把一张图片发送给 Google Gemini 视觉模型（免费版）做识别，返回对图片的文字描述、画面理解或提取文字。主模型遇到图片时用它补齐视觉能力。",
        args: {
          image: tool.schema.string().describe(
            "要识别的图片：本地路径、http(s) URL、或 data:image/...;base64,... 数据URI"
          ),
          prompt: tool.schema.string().optional().describe(
            "可选的任务指令，例如'描述这张图片'、'提取图中文字'、'翻译图中的英文'。默认自动描述。"
          ),
          model: tool.schema.string().optional().describe(
            "可选的自定义模型ID，默认读 GEMINI_VISION_MODEL，未设则用 gemini-flash-latest"
          ),
        },
        async execute(args) {
          const apiKey = process.env.GEMINI_API_KEY
          if (!apiKey) {
            return "未配置 GEMINI_API_KEY 环境变量。请到 https://aistudio.google.com/apikey 申请免费 key，然后 export GEMINI_API_KEY=xxx 并重启 opencode。"
          }

          const model =
            args.model ?? process.env.GEMINI_VISION_MODEL ?? "gemini-flash-latest"
          const instruction =
            args.prompt ??
            "请仔细观察这张图片，用中文详细描述图片里的场景、物体、文字和关键信息。"

          try {
            return await geminiRequest(
              [{ inlineData: imageAsInline(args.image) }],
              apiKey,
              model,
              instruction
            )
          } catch (err) {
            return `Gemini 识别出错：${err instanceof Error ? err.message : String(err)}`
          }
        },
      }),
    },
  }
}
