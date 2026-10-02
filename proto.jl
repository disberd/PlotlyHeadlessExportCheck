const T0 = time()
# PROTOTYPE, throwaway. Not production code. Port of the ticket 06 prototype to Linux, macOS and
# Windows. Exports PlotlyBaseExtras plots through chrome-headless-shell over CDP on
# --remote-debugging-pipe and measures time, memory and CPU of the browser process tree.
#
# Run from the repo root (CHROME_PATH points to the chrome-headless-shell executable):
#   julia --project=. --startup-file=no proto.jl probe
#   julia --project=. --startup-file=no proto.jl bench [variants...]
#   julia --project=. --startup-file=no proto.jl ttfx
#   julia --project=. --startup-file=no proto.jl grow [strategies...]
#   julia --project=. --startup-file=no proto.jl hold [error]
# Variants: A_inline A_file B_inject H_inject C_floor. ENV: REPS (default 6), IDLE (s, default 20), N (grow, default 120).
using PlotlyBaseExtras, JSON, Base64
import PlotlyBase: scatter, scattergl, GenericTrace, Layout
const PBE = PlotlyBaseExtras
const T_USING = time() - T0

const HERE = @__DIR__
const CHROME = get(ENV, "CHROME_PATH", joinpath(HERE, "browser", "chrome-headless-shell-linux64", "chrome-headless-shell"))
const OUT = mkpath(joinpath(HERE, "out"))
const WORK = mkpath(joinpath(HERE, "work"))
const CLK_TCK = Sys.islinux() ? ccall(:sysconf, Clong, (Cint,), 2) : 1  # _SC_CLK_TCK

# file:// URL of a local path. Windows paths need file:///C:/...
fileurl(path) = Sys.iswindows() ? "file:///" * replace(path, '\\' => '/') : "file://" * path
const REPS = parse(Int, get(ENV, "REPS", "6"))
const IDLE = parse(Float64, get(ENV, "IDLE", "20"))

med(v) = isempty(v) ? NaN : (s = sort(v); n = length(s); isodd(n) ? s[(n + 1) ÷ 2] : (s[n ÷ 2] + s[n ÷ 2 + 1]) / 2)
ms(t) = round(1000t; digits = 1)

# ---------- CDP over the pipe transport (from research/spikes/03/pipe_spike.jl) ----------

function child_pipe(child_reads::Bool)
    rd, wr = Base.link_pipe(!child_reads, child_reads)
    parent = Base.open_pipe!(Base.PipeEndpoint(), child_reads ? wr : rd)
    return parent, (child_reads ? rd : wr)
end

mutable struct Browser
    proc::Base.Process
    to::Base.PipeEndpoint
    from::Base.PipeEndpoint
    next_id::Int
    pending::Dict{Int,Channel{Any}}
    errors::Vector{String}
    lk::ReentrantLock
    profile::String
end

const FLAGS = ["--headless", "--no-first-run", "--no-default-browser-check", "--disable-breakpad",
    "--enable-unsafe-swiftshader", "--allow-file-access-from-files", "--disable-dev-shm-usage",
    "--hide-scrollbars", "--mute-audio", "--disable-background-networking", "--disable-extensions",
    "--disable-sync", "--disable-background-timer-throttling", "--disable-renderer-backgrounding",
    "--disable-backgrounding-occluded-windows"]

const NSTART = Ref(0)
function start_browser(flags = FLAGS)
    profile = mktempdir()
    to, child_in = child_pipe(true)
    from, child_out = child_pipe(false)
    cmd = Cmd([CHROME; flags; "--user-data-dir=$profile"; "--remote-debugging-pipe"; "about:blank"])
    red = Base.CmdRedirect(Base.CmdRedirect(cmd, child_in, 3), child_out, 4)
    errlog = joinpath(OUT, "chrome-stderr-$(getpid())-$(NSTART[] += 1).log")
    proc = run(pipeline(red; stdin = devnull, stdout = devnull, stderr = errlog); wait = false)
    Base.close_pipe_sync(child_in)
    Base.close_pipe_sync(child_out)
    b = Browser(proc, to, from, 0, Dict{Int,Channel{Any}}(), String[], ReentrantLock(), profile)
    errormonitor(@async reader(b))
    return b
end

function reader(b::Browser)
    while isopen(b.from)
        raw = try readuntil(b.from, '\0') catch; break end
        isempty(raw) && (eof(b.from) ? break : continue)
        msg = JSON.parse(raw)
        if haskey(msg, "id")
            ch = lock(() -> get(b.pending, msg["id"], nothing), b.lk)
            ch === nothing || put!(ch, msg)
        elseif get(msg, "method", "") == "Runtime.exceptionThrown"
            push!(b.errors, string(get(msg["params"]["exceptionDetails"], "exception", msg["params"]["exceptionDetails"])))
        elseif get(msg, "method", "") == "Runtime.consoleAPICalled" && msg["params"]["type"] == "error"
            push!(b.errors, join((string(get(a, "description", get(a, "value", ""))) for a in msg["params"]["args"]), " "))
        end
    end
end

function cdp(b::Browser, method, params = Dict{String,Any}(); session = nothing, timeout = 60.0)
    ch = Channel{Any}(1)
    id = lock(b.lk) do
        b.next_id += 1
        b.pending[b.next_id] = ch
        b.next_id
    end
    msg = Dict{String,Any}("id" => id, "method" => method, "params" => params)
    session === nothing || (msg["sessionId"] = session)
    write(b.to, JSON.json(msg), '\0')
    timer = Timer(_ -> close(ch), timeout)
    r = try
        take!(ch)
    catch
        error("CDP $method timed out after $timeout s")
    finally
        close(timer)
        lock(() -> delete!(b.pending, id), b.lk)
    end
    haskey(r, "error") && error("CDP $method: $(r["error"])")
    return r["result"]
end

function evaluate(b, session, expr; timeout = 60.0)
    r = cdp(b, "Runtime.evaluate", Dict("expression" => expr, "awaitPromise" => true, "returnByValue" => true); session, timeout)
    haskey(r, "exceptionDetails") && error("JS: " * string(get(r["exceptionDetails"], "exception", r["exceptionDetails"])))
    return get(r["result"], "value", nothing)
end

const TARGETS = Dict{String,String}()  # session id => target id
function new_page(b; runtime = true)
    tid = cdp(b, "Target.createTarget", Dict("url" => "about:blank"))["targetId"]
    s = cdp(b, "Target.attachToTarget", Dict("targetId" => tid, "flatten" => true))["sessionId"]
    TARGETS[s] = tid
    runtime && cdp(b, "Runtime.enable"; session = s)
    cdp(b, "Page.enable"; session = s)
    return s
end

function stop_browser(b)
    close(b.to)  # EOF on fd 3: Chrome closes itself
    timedwait(() -> process_exited(b.proc), 10.0) === :timed_out && kill(b.proc)
    try rm(b.profile; recursive = true, force = true) catch e; @warn "profile not removed" e end
end

# Wait until the page publishes `token`. Retries while a navigation replaces the context.
function wait_token(b, s, token; timeout = 30)
    expr = """new Promise((res, rej) => { const t0 = performance.now(); (function poll() {
        if (window.__export_ready === $(repr(token))) return res(true);
        if (performance.now() - t0 > $(1000timeout)) return rej(new Error('ready timeout'));
        setTimeout(poll, 1); })(); })"""
    for _ in 1:20
        try
            return evaluate(b, s, expr; timeout = timeout + 5)
        catch e
            occursin(r"destroyed|Cannot find context|context with specified id", sprint(showerror, e)) || rethrow()
            sleep(0.005)
        end
    end
    error("page never became ready")
end

# ---------- process tree stats ----------
# One row per process. cpu: ticks on Linux, seconds elsewhere (CLK_TCK = 1). rss and pss in kB.
# `pss` is a total that counts shared pages once: PSS on Linux, the physical footprint (top MEM)
# on macOS, the private bytes on Windows.

const PSS_NAME = Sys.islinux() ? "PSS" : Sys.isapple() ? "footprint" : "private"
const RSS_NAME = Sys.iswindows() ? "WS" : "RSS"

ptype(cmd) = (m = match(r"--type=(\S+)", cmd); m === nothing ? "browser" : m[1])

# macOS ps time: [dd-][hh:]mm:ss.ss
function cputime(s)
    d, hms = occursin('-', s) ? split(s, '-') : ("0", s)
    return 86400 * parse(Int, d) + foldl((a, x) -> 60a + parse(Float64, x), split(hms, ':'); init = 0.0)
end

const KB = Dict('B' => 1 / 1024, 'K' => 1, 'M' => 1024, 'G' => 1024^2)

function proc_table()
    t = Dict{Int,Any}()
    if Sys.islinux()
        for d in readdir("/proc")
            pid = tryparse(Int, d)
            pid === nothing && continue
            try
                st = read("/proc/$pid/stat", String)
                f = split(st[findlast(')', st)+2:end])  # f[2] = ppid, f[12..15] = utime stime cutime cstime
                m = match(r"VmRSS:\s+(\d+)", read("/proc/$pid/status", String))
                sm = try read("/proc/$pid/smaps_rollup", String) catch; "" end
                mp = match(r"^Pss:\s+(\d+)"m, sm)
                t[pid] = (; pid, ppid = parse(Int, f[2]), cpu = sum(parse(Int, f[k]) for k in 12:15),
                    rss = m === nothing ? 0 : parse(Int, m[1]), pss = mp === nothing ? 0 : parse(Int, mp[1]),
                    typ = ptype(replace(read("/proc/$pid/cmdline", String), '\0' => ' ')))
            catch
            end
        end
    elseif Sys.isapple()
        foot = Dict{Int,Float64}()
        for l in eachline(`top -l 1 -s 0 -stats pid,mem`)
            m = match(r"^\s*(\d+)\s+([\d.]+)([BKMG])", l)
            m === nothing || (foot[parse(Int, m[1])] = parse(Float64, m[2]) * KB[m[3][1]])
        end
        for l in eachline(`ps -A -o pid=,ppid=,rss=,time=,command=`)
            f = split(strip(l), r"\s+"; limit = 5)
            pid = parse(Int, f[1])
            t[pid] = (; pid, ppid = parse(Int, f[2]), cpu = cputime(f[4]), rss = parse(Int, f[3]),
                pss = round(Int, get(foot, pid, 0.0)), typ = ptype(get(f, 5, "")))
        end
    else
        ps = raw"Get-CimInstance Win32_Process | % { '{0} {1} {2} {3} {4} {5}' -f $_.ProcessId, $_.ParentProcessId, $_.WorkingSetSize, $_.PrivatePageCount, ($_.KernelModeTime + $_.UserModeTime), $_.CommandLine }"
        for l in eachline(`powershell -NoProfile -NonInteractive -Command $ps`)
            f = split(strip(l), ' '; limit = 6)
            length(f) >= 5 || continue
            pid = tryparse(Int, f[1])
            pid === nothing && continue
            t[pid] = (; pid, ppid = parse(Int, f[2]), cpu = parse(Int, f[5]) / 1e7, rss = parse(Int, f[3]) ÷ 1024,
                pss = parse(Int, f[4]) ÷ 1024, typ = ptype(get(f, 6, "")))
        end
    end
    return t
end

function tree(root, t = proc_table())
    kids = Dict{Int,Vector{Int}}()
    for r in values(t)
        r.pid == r.ppid || push!(get!(kids, r.ppid, Int[]), r.pid)
    end
    out = [root]
    i = 1
    while i <= length(out)
        append!(out, get(kids, out[i], Int[]))
        i += 1
    end
    return out
end

function tree_stats(b)
    t = proc_table()
    return [t[p] for p in tree(getpid(b.proc), t) if haskey(t, p)]
end
cpu_s(stats) = sum(s.cpu for s in stats) / CLK_TCK
function mem_line(stats)
    rss = round(sum(s.rss for s in stats) / 1024; digits = 1)
    pss = round(sum(s.pss for s in stats) / 1024; digits = 1)
    bytype = Dict{String,Tuple{Int,Float64}}()
    for s in stats
        n, p = get(bytype, s.typ, (0, 0.0))
        bytype[s.typ] = (n + 1, p + s.pss / 1024)
    end
    parts = join(("$t×$n $(round(p; digits = 1))" for (t, (n, p)) in sort(collect(bytype))), ", ")
    return "procs=$(length(stats)) $RSS_NAME=$(rss) MB $PSS_NAME=$(pss) MB [$PSS_NAME by type: $parts]"
end

# ---------- figures (PlotlyPlot with a ready signal) ----------

# PROTOTYPE ready signal. PBE attaches plotly listeners after `Plotly.react` resolves, so a probe
# listener tells the user script that the first draw is done. Then the user customization runs,
# then the script publishes the token.
function with_ready!(p, token; extra = "")
    PBE.add_plotly_listener!(p, "plotly_export_probe", "() => {}")
    PBE.push_script!(p, """
    await new Promise((res, rej) => { const t0 = performance.now(); (function poll() {
      if (PLOT._ev?.listeners?.('plotly_export_probe')?.length) return res();
      if (performance.now() - t0 > 15000) return rej(new Error('probe timeout'));
      setTimeout(poll, 1); })(); });
    $extra
    window.__export_ready = $(repr(token));
    """)
    return p
end

function fig_basic(token)
    p = PBE.plot([scatter(y = cumsum(randn(50)), name = "walk")],
        Layout(title = "\$\\text{Export test: } \\alpha^2 + \\beta_1\$", width = 700, height = 450))
    # A listener changes the plot through the Plotly API on the next draw.
    PBE.add_plotly_listener!(p, "plotly_afterplot", """
    function () {
      if (PLOT.__restyled) return;
      PLOT.__restyled = true;
      PLOT.__restyle_promise = Plotly.restyle(PLOT, {'line.color': 'crimson', 'line.width': 4});
    }""")
    # A user script changes the layout through the Plotly API, then waits for the listener.
    return with_ready!(p, token; extra = """
    await Plotly.relayout(PLOT, {paper_bgcolor: '#fff3e0', 'title.font.color': '#1565c0'});
    { const t0 = performance.now();
      while (!PLOT.__restyle_promise && performance.now() - t0 < 1000) await new Promise(r => setTimeout(r, 2)); }
    await PLOT.__restyle_promise;
    """)
end

fig_typical(token) = with_ready!(PBE.plot([scatter(x = 1:1000, y = cumsum(randn(1000)), name = "s$i") for i in 1:4],
    Layout(title = "Typical: 4 traces × 1000 points", width = 900, height = 500)), token)

fig_gl(token) = with_ready!(PBE.plot([scattergl(x = randn(100_000), y = randn(100_000), mode = "markers", marker_size = 2)],
    Layout(title = "scattergl: 100k points", width = 800, height = 600)), token)

fig_map(token) = with_ready!(PBE.plot([GenericTrace("scattermap"; lat = 40 .+ 5randn(200), lon = 10 .+ 5randn(200), mode = "markers")],
    Layout(title = "scattermap (tiles need network)", width = 800, height = 600, map_zoom = 3,
        map_center = Dict(:lat => 40, :lon => 10))), token)

const FIGS = [("basic", fig_basic), ("typical", fig_typical), ("gl100k", fig_gl), ("map", fig_map)]
fig_size(p) = (p.Plot.layout[:width], p.Plot.layout[:height])

# ---------- page sources ----------

const PLOTLY_FILE = fileurl(PBE.get_local_path(PBE.get_plotly_version()))
const MATHJAX_FILE = let v = PBE.get_mathjax_version()
    PBE.maybe_add_mathjax_local(v)
    fileurl(PBE.get_local_mathjax_path(v))
end

# PROTOTYPE headless adapter: mount like the plain adapter, and keep the teardown so that the
# headless host can free the plot after the export.
struct HeadlessHost <: PBE.Host end
PBE.adapter_script(::HeadlessHost) = """
const { container: CONTAINER, teardown } = renderPlot({ plot_obj, Plotly, css, plotly_listeners, js_listeners });
const PLOT = CONTAINER.PLOT;
currentScript.insertAdjacentElement("beforebegin", CONTAINER);
window.__pbe_free = () => {
  teardown();
  destroyContainer(CONTAINER);
  CONTAINER.remove();
  currentScript.remove();
  window.__pbe_free = undefined;
};"""

function fragment(p, source; host = PBE.PlainHTML())
    if source === :inline
        return PBE.ScopedValues.with(PBE.plotly_source => :inline, PBE.mathjax_source => :inline) do
            sprint(io -> PBE.render(io, host, p))
        end
    end
    s = PBE.ScopedValues.with(PBE.plotly_source => :cdn, PBE.mathjax_source => :cdn) do
        sprint(io -> PBE.render(io, host, p))
    end
    s = replace(s, PBE.get_plotly_esm_url(PBE.get_plotly_version()) => PLOTLY_FILE,
        PBE.mathjax_cdn_url(PBE.get_mathjax_version()) => MATHJAX_FILE)
    occursin(r"esm\.sh|jsdelivr", s) && error("a CDN URL is left in the page")
    return s
end

page(body; head = "") = "<!doctype html><html><head><meta charset='utf-8'>$head</head><body style='margin:0'>\n$body\n</body></html>"

const FLOOR_HEAD = """
<script>window.MathJax = { svg: { fontCache: "local" }, startup: { typeset: false } };</script>
<script src="$MATHJAX_FILE"></script>
<script type="module">
import Plotly from "$PLOTLY_FILE";
window.Plotly = Plotly;
await MathJax.startup.promise;
window.__export_ready = "floor";
</script>"""

const EXPORT_PBE = """(async () => {
  const C = document.querySelector('.plotlyplot-container');
  const url = await C.Plotly.toImage(C.PLOT, {format: FMT, width: W, height: H, scale: 1});
  const i = url.indexOf(',');
  return {body: FMT === 'svg' ? decodeURIComponent(url.slice(i + 1)) : url.slice(i + 1),
          restyled: !!C.PLOT.__restyled, bg: C.PLOT.layout.paper_bgcolor ?? null};
})()"""

const CLEANUP_INJECT = """(() => {
  const C = document.querySelector('.plotlyplot-container');
  for (const c of C.querySelectorAll('canvas')) {
    const gl = c.getContext('webgl2') ?? c.getContext('webgl');
    gl?.getExtension('WEBGL_lose_context')?.loseContext();
  }
  C.Plotly.purge(C.PLOT);
  C.remove();
  document.getElementById('injected-plot')?.remove();
  return true;
})()"""

# ---------- one export per variant ----------

mutable struct Session
    variant::String
    b::Browser
    s::String
    n::Int
end

function setup_page!(S::Session)
    if S.variant in ("B_inject", "H_inject", "C_floor")
        path = joinpath(WORK, "$(S.variant)-base.html")
        write(path, page(""; head = S.variant == "C_floor" ? FLOOR_HEAD : ""))
        cdp(S.b, "Page.navigate", Dict("url" => fileurl(path)); session = S.s)
        S.variant == "C_floor" ? wait_token(S.b, S.s, "floor") :
            wait_until(S, "document.readyState === 'complete'")
    end
end
wait_until(S, cond) = evaluate(S.b, S.s, """new Promise(r => { (function p() { ($cond) ? r(true) : setTimeout(p, 2); })(); })""")

# Returns (bytes_or_string, timings (julia, load, image), flags)
function export_one(S::Session, figf, fmt)
    S.n += 1
    token = "t$(S.n)"
    t0 = time()
    p = figf(token)
    w, h = fig_size(p)
    v = S.variant
    if v == "C_floor"
        json = sprint(PBE.write_js, PBE._process_with_names(p))
        t1 = time()
        expr = """(async () => {
          const url = await Plotly.toImage($json, {format: '$fmt', width: $w, height: $h, scale: 1});
          const i = url.indexOf(',');
          return {body: '$fmt' === 'svg' ? decodeURIComponent(url.slice(i + 1)) : url.slice(i + 1), restyled: false, bg: null};
        })()"""
        t2 = time()
        r = evaluate(S.b, S.s, expr)
        t3 = time()
        return decode(r, fmt), (t1 - t0, 0.0, t3 - t2), r
    end
    frag = fragment(p, v == "A_inline" ? :inline : :file; host = startswith(v, "H_") ? HeadlessHost() : PBE.PlainHTML())
    if v in ("B_inject", "H_inject")
        js = match(r"^<script id='[^']*'>(.*)</script>\s*$"s, frag)[1]
        t1 = time()
        evaluate(S.b, S.s, """(() => { const s = document.createElement('script'); s.id = 'injected-plot';
            s.textContent = $(JSON.json(js)); document.body.appendChild(s); return true; })()""")
    else
        path = joinpath(WORK, "$v-page.html")
        write(path, page(frag))
        t1 = time()
        cdp(S.b, "Page.navigate", Dict("url" => fileurl(path) * "?$token"); session = S.s)
    end
    wait_token(S.b, S.s, token)
    t2 = time()
    r = evaluate(S.b, S.s, replace(EXPORT_PBE, "FMT" => repr(fmt), "W" => string(w), "H" => string(h)))
    t3 = time()
    v == "B_inject" && evaluate(S.b, S.s, CLEANUP_INJECT)
    v == "H_inject" && evaluate(S.b, S.s, "(window.__pbe_free(), true)")
    return decode(r, fmt), (t1 - t0, t2 - t1, t3 - t2), r
end

decode(r, fmt) = fmt == "svg" ? r["body"] : base64decode(r["body"])

# ---------- modes ----------

function bench(variant)
    println("\n## $variant")
    t0 = time()
    b = start_browser()
    ver = cdp(b, "Browser.getVersion")["product"]
    t_start = time() - t0
    S = Session(variant, b, new_page(b), 0)
    setup_page!(S)
    t_ready = time() - t0
    st = tree_stats(b)
    cpu0 = cpu_s(st)
    println("browser: $ver")
    println("cold start (spawn → first CDP reply): $(ms(t_start)) ms; page ready: $(ms(t_ready)) ms; CPU so far $(round(cpu0; digits = 2)) s")
    println("memory after start: ", mem_line(st))

    # First export: cold page.
    t1 = time()
    img, (tj, tl, ti), r = export_one(S, fig_basic, "png")
    t_first = time() - t1
    write(joinpath(OUT, "$variant-first-basic.png"), img)
    println("first export (basic, png): $(ms(t_first)) ms [julia $(ms(tj)), load+scripts $(ms(tl)), toImage $(ms(ti))]; user script bg=$(r["bg"]) listener restyled=$(r["restyled"])")
    println("cold total (spawn → first PNG bytes): $(ms(t_ready + t_first)) ms")

    # Warm exports.
    cpu1 = cpu_s(tree_stats(b))
    tw0 = time()
    nexp = 0
    for (name, f) in FIGS
        tot, js, ls, is = Float64[], Float64[], Float64[], Float64[]
        err = nothing
        for i in 1:REPS
            try
                t = @elapsed (img, (tj, tl, ti), _) = export_one(S, f, "png")
                push!(tot, t); push!(js, tj); push!(ls, tl); push!(is, ti)
                i == 1 && write(joinpath(OUT, "$variant-$name.png"), img)
                nexp += 1
            catch e
                err = first(sprint(showerror, e), 200)
                break
            end
        end
        println("warm $(rpad(name, 8)) png median $(ms(med(tot))) ms [julia $(ms(med(js))), load+scripts $(ms(med(ls))), toImage $(ms(med(is)))] n=$(length(tot))",
            err === nothing ? "" : "  ERROR: $err")
    end
    tw = time() - tw0
    st = tree_stats(b)
    cpu2 = cpu_s(st)
    println("warm loop: $nexp exports in $(round(tw; digits = 2)) s; browser CPU $(round(cpu2 - cpu1; digits = 2)) s (= $(round(100(cpu2 - cpu1) / tw; digits = 1)) % of one core)")
    println("memory after warm loop: ", mem_line(st))

    # All formats of the basic figure.
    for fmt in ("png", "jpeg", "svg", "webp")
        t = @elapsed (img, _, r) = export_one(S, fig_basic, fmt)
        ext = fmt == "jpeg" ? "jpg" : fmt
        fn = joinpath(OUT, "$variant-basic.$ext")
        fmt == "svg" ? write(fn, img) : write(fn, img)
        extra = fmt == "svg" ? " bg=$(occursin("rgb(255, 243, 224)", img)) crimson=$(occursin("rgb(220, 20, 60)", img)) math=$(occursin("math-group", img))" : ""
        println("format $(rpad(fmt, 4)) $(ms(t)) ms, $(round(filesize(fn) / 1024; digits = 1)) kB$extra")
    end

    # Idle.
    c0 = cpu_s(tree_stats(b))
    sleep(IDLE)
    st = tree_stats(b)
    println("idle $(IDLE) s: browser CPU $(round(cpu_s(st) - c0; digits = 3)) s; memory ", mem_line(st))
    isempty(b.errors) || println("page errors ($(length(b.errors))): ", first(b.errors, 3))
    stop_browser(b)
    println("stopped: exited=$(process_exited(b.proc))")
end

function ttfx()
    println("T_USING=$(round(T_USING; digits = 2)) s")
    t0 = time()
    b = start_browser()
    cdp(b, "Browser.getVersion")
    S = Session("A_file", b, new_page(b), 0)
    t1 = time()
    img, _, _ = export_one(S, fig_basic, "png")
    write(joinpath(OUT, "ttfx.png"), img)
    t2 = time()
    println("T_BROWSER_START=$(round(t1 - t0; digits = 2)) s T_FIRST_EXPORT=$(round(t2 - t1; digits = 2)) s T_SCRIPT_TOTAL=$(round(t2 - T0; digits = 2)) s")
    t3 = time()
    export_one(S, fig_basic, "png")
    println("T_SECOND_EXPORT=$(round(time() - t3; digits = 3)) s")
    stop_browser(b)
end

function hold(mode)
    b = start_browser()
    cdp(b, "Browser.getVersion")
    S = Session("A_file", b, new_page(b), 0)
    export_one(S, fig_basic, "png")
    println("CHROME_PID=$(getpid(b.proc))")
    println("PROFILE=$(b.profile)")
    println("TREE=$(join(tree(getpid(b.proc)), ','))")
    println("READY")
    flush(stdout)
    mode == "error" && error("uncaught exception after READY")
    sleep(600)
end

# How much memory does closing the page free, compared with keeping it open?
function mem()
    b = start_browser()
    cdp(b, "Browser.getVersion")
    println("start:                 ", mem_line(tree_stats(b)))
    tid = cdp(b, "Target.createTarget", Dict("url" => "about:blank"))["targetId"]
    s = cdp(b, "Target.attachToTarget", Dict("targetId" => tid, "flatten" => true))["sessionId"]
    cdp(b, "Runtime.enable"; session = s)
    S = Session("A_file", b, s, 0)
    for (_, f) in FIGS, _ in 1:3
        try export_one(S, f, "png") catch e; println("skip: ", first(sprint(showerror, e), 120)) end
    end
    sleep(2)
    println("after 12 exports:      ", mem_line(tree_stats(b)))
    cdp(b, "Page.navigate", Dict("url" => "about:blank"); session = s)
    sleep(2)
    println("page at about:blank:   ", mem_line(tree_stats(b)))
    cdp(b, "Target.closeTarget", Dict("targetId" => tid))
    sleep(2)
    println("page closed:           ", mem_line(tree_stats(b)))
    S.s = new_page(b)
    t = @elapsed export_one(S, fig_basic, "png")
    println("new page + basic export: $(ms(t)) ms")
    stop_browser(b)
end

# ---------- memory growth over many exports ----------

fig_gl20(token) = with_ready!(PBE.plot([scattergl(x = randn(20_000), y = randn(20_000), mode = "markers", marker_size = 3)],
    Layout(title = "scattergl: 20k points", width = 800, height = 600)), token)
const GROW_FIGS = [fig_typical, fig_basic, fig_gl20]

function page_metrics(b, s)
    m = try cdp(b, "Performance.getMetrics"; session = s)["metrics"] catch; return "metrics n/a" end
    d = Dict(x["name"] => x["value"] for x in m)
    return "jsheap=$(round(d["JSHeapUsedSize"] / 2^20; digits = 1)) MB nodes=$(Int(d["Nodes"])) listeners=$(Int(d["JSEventListeners"])) docs=$(Int(d["Documents"]))"
end

function pss_line(b)
    st = tree_stats(b)
    by(t) = round(Int, sum((s.pss for s in st if s.typ == t); init = 0) / 1024)
    return "PSS total=$(round(Int, sum(s.pss for s in st) / 1024)) renderer=$(by("renderer")) gpu=$(by("gpu-process")) browser=$(by("browser")) MB"
end

const STRATEGIES = Dict(
    "purge" => (variant = "B_inject", runtime = true, gc = false, pressure = false, recycle = 0),
    "teardown" => (variant = "H_inject", runtime = true, gc = false, pressure = false, recycle = 0),
    "teardown_noruntime" => (variant = "H_inject", runtime = false, gc = false, pressure = false, recycle = 0),
    "teardown_gc" => (variant = "H_inject", runtime = true, gc = true, pressure = false, recycle = 0),
    "pressure_each" => (variant = "H_inject", runtime = true, gc = true, pressure = true, recycle = 0),
    "navigate" => (variant = "H_file", runtime = true, gc = false, pressure = false, recycle = 0),
    "recycle40" => (variant = "H_inject", runtime = true, gc = false, pressure = false, recycle = 40),
)

function release!(b, s)
    t = @elapsed begin
        cdp(b, "HeapProfiler.collectGarbage"; session = s)
        cdp(b, "Memory.simulatePressureNotification", Dict("level" => "critical"); session = s)
    end
    return t
end

function grow(name; n = parse(Int, get(ENV, "N", "120")), every = 20)
    cfg = STRATEGIES[name]
    println("\n## $name ($(cfg.variant), runtime=$(cfg.runtime), gc=$(cfg.gc), recycle=$(cfg.recycle))")
    b = start_browser()
    cdp(b, "Browser.getVersion")
    open_page() = (s = new_page(b; runtime = cfg.runtime); cdp(b, "Performance.enable"; session = s); s)
    S = Session(cfg.variant, b, open_page(), 0)
    setup_page!(S)
    println("   0 exports: ", pss_line(b), "  ", page_metrics(b, S.s))
    times = Float64[]
    rel = Float64[]
    for i in 1:n
        if cfg.recycle > 0 && i > 1 && (i - 1) % cfg.recycle == 0
            cdp(b, "Target.closeTarget", Dict("targetId" => TARGETS[S.s]))
            S.s = open_page()
            setup_page!(S)
        end
        push!(times, @elapsed export_one(S, GROW_FIGS[mod1(i, 3)], "png"))
        if cfg.pressure
            push!(rel, release!(b, S.s))
        elseif cfg.gc
            cdp(b, "HeapProfiler.collectGarbage"; session = S.s)
        end
        i % every == 0 && println(lpad(i, 4), " exports: ", pss_line(b), "  ", page_metrics(b, S.s),
            "  median $(ms(med(times[i-every+1:i]))) ms")
    end
    sleep(5)
    println("idle 5 s:     ", pss_line(b), "  ", page_metrics(b, S.s))
    isempty(rel) || println("release per export (gc + pressure): median $(ms(med(rel))) ms")
    t = release!(b, S.s)
    sleep(3)
    println("gc+pressure:  ", pss_line(b), "  ", page_metrics(b, S.s), "  (release took $(ms(t)) ms)")
    after = [@elapsed(export_one(S, f, "png")) for f in GROW_FIGS]
    println("exports right after the release: ", join(ms.(after), " / "), " ms (typical / basic / gl20k)")
    du = sum((filesize(joinpath(r, f)) for (r, _, fs) in walkdir(b.profile) for f in fs); init = 0)
    println("profile dir on disk: $(round(du / 2^20; digits = 1)) MB")
    isempty(b.errors) || println("page errors: $(length(b.errors)); first: ", first(first(b.errors), 160))
    stop_browser(b)
end

# ---------- WebGL and MathJax per flag set ----------

const PROBE_FLAGS = [
    "default" => FLAGS,
    "no-unsafe-swiftshader" => filter(!=("--enable-unsafe-swiftshader"), FLAGS),
    "angle-swiftshader" => [FLAGS; "--use-angle=swiftshader"],
    "disable-gpu" => [FLAGS; "--disable-gpu"],
]

# Red markers on white with no axes: the red pixel count shows whether the points were drawn.
# scatter_svg is the reference without WebGL. The map style needs no tiles.
const PROBE_JS = raw"""(async () => {
  const c = document.createElement('canvas');
  const gl = c.getContext('webgl2') ?? c.getContext('webgl');
  const dbg = gl?.getExtension('WEBGL_debug_renderer_info');
  const renderer = gl ? String(gl.getParameter(dbg ? dbg.UNMASKED_RENDERER_WEBGL : gl.RENDERER)) : null;
  async function red(url) {
    const img = new Image(); img.src = url; await img.decode();
    const k = document.createElement('canvas'); k.width = img.width; k.height = img.height;
    const x = k.getContext('2d'); x.drawImage(img, 0, 0);
    const d = x.getImageData(0, 0, k.width, k.height).data;
    let n = 0;
    for (let i = 0; i < d.length; i += 4) if (d[i] > 200 && d[i + 1] < 60 && d[i + 2] < 60) n++;
    return n;
  }
  const xs = Array.from({length: 200}, (_, i) => Math.cos(i) * (1 + i / 200));
  const ys = Array.from({length: 200}, (_, i) => Math.sin(i) * (1 + i / 200));
  const marker = {color: 'rgb(255,0,0)', size: 10};
  const ax = {visible: false};
  const base = {width: 600, height: 400, margin: {l: 0, r: 0, t: 0, b: 0}, xaxis: ax, yaxis: ax, showlegend: false};
  const figs = {
    scatter_svg: {data: [{type: 'scatter', x: xs, y: ys, mode: 'markers', marker}], layout: base},
    scattergl: {data: [{type: 'scattergl', x: xs, y: ys, mode: 'markers', marker}], layout: base},
    scattermap: {data: [{type: 'scattermap', lat: ys.map(v => 40 + 10 * v), lon: xs.map(v => 10 + 10 * v), mode: 'markers', marker}],
                 layout: {...base, map: {style: 'white-bg', zoom: 2, center: {lat: 40, lon: 10}}}},
  };
  const out = {renderer};
  for (const [name, fig] of Object.entries(figs)) {
    try {
      const t0 = performance.now();
      const url = await Plotly.toImage(fig, {format: 'png', width: 600, height: 400, scale: 1});
      out[name] = {ms: Math.round(performance.now() - t0), red: await red(url), png: url.slice(url.indexOf(',') + 1)};
    } catch (e) { out[name] = {error: String(e)}; }
  }
  try {
    const fig = {data: [{type: 'scatter', y: [1, 3, 2]}], layout: {width: 600, height: 400, title: {text: '$\\alpha^2 + \\beta_1$'}}};
    const url = await Plotly.toImage(fig, {format: 'svg', width: 600, height: 400});
    out.mathjax = {math: decodeURIComponent(url.slice(url.indexOf(',') + 1)).includes('math-group')};
  } catch (e) { out.mathjax = {error: String(e)}; }
  return out;
})()"""

function probe()
    path = joinpath(WORK, "probe.html")
    write(path, page(""; head = FLOOR_HEAD))
    for (name, flags) in PROBE_FLAGS
        b = start_browser(flags)
        try
            ver = cdp(b, "Browser.getVersion")["product"]
            s = new_page(b)
            cdp(b, "Page.navigate", Dict("url" => fileurl(path)); session = s)
            wait_token(b, s, "floor")
            r = evaluate(b, s, PROBE_JS; timeout = 120)
            parts = String[]
            for k in ("scatter_svg", "scattergl", "scattermap")
                x = r[k]
                if haskey(x, "error")
                    push!(parts, "$k ERROR $(first(x["error"], 120))")
                else
                    write(joinpath(OUT, "probe-$name-$k.png"), base64decode(x["png"]))
                    push!(parts, "$k red=$(x["red"]) $(x["ms"]) ms")
                end
            end
            println("probe $(rpad(name, 22)) $ver; WebGL: $(r["renderer"]); ", join(parts, "; "),
                "; mathjax=$(get(r["mathjax"], "math", r["mathjax"]))",
                isempty(b.errors) ? "" : "; page errors $(length(b.errors)): $(first(first(b.errors), 160))")
        catch e
            println("probe $(rpad(name, 22)) FAILED: ", first(sprint(showerror, e), 300))
        finally
            stop_browser(b)
        end
    end
end

let mode = get(ARGS, 1, "bench")
    if mode == "probe"
        probe()
    elseif mode == "bench"
        for v in (length(ARGS) > 1 ? ARGS[2:end] : ["A_inline", "A_file", "B_inject", "C_floor"])
            bench(v)
        end
    elseif mode == "ttfx"
        ttfx()
    elseif mode == "hold"
        hold(get(ARGS, 2, ""))
    elseif mode == "mem"
        mem()
    elseif mode == "grow"
        for s in (length(ARGS) > 1 ? ARGS[2:end] : ["purge", "teardown", "teardown_noruntime", "teardown_gc", "navigate", "recycle40"])
            grow(s)
        end
    end
end
