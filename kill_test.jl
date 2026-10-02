# PROTOTYPE, throwaway. Starts `proto.jl hold` in a child Julia, ends that Julia with SIGKILL
# (TerminateProcess on Windows, same as `taskkill /F`) or with an uncaught exception, and counts
# the browser processes that stay alive.
# Run from the repo root: julia --project=. --startup-file=no kill_test.jl
const HERE = @__DIR__
const OUT = mkpath(joinpath(HERE, "out"))

win_pids() = Set(parse(Int, strip(split(l, ',')[2], '"')) for l in eachline(`tasklist /FO CSV /NH`) if count(==(','), l) >= 2)
living(pids) = Sys.iswindows() ? (s = win_pids(); count(in(s), pids)) :
    count(p -> ccall(:kill, Cint, (Cint, Cint), p, 0) == 0, pids)
zap(p) = Sys.iswindows() ? run(ignorestatus(pipeline(`taskkill /F /PID $p`; stdout = devnull, stderr = devnull))) :
    ccall(:kill, Cint, (Cint, Cint), p, 9)

for mode in ("kill9", "error")
    log = joinpath(OUT, "hold-$mode.log")
    io = open(log, "w")
    cmd = `$(Base.julia_cmd()) --project=$HERE --startup-file=no $(joinpath(HERE, "proto.jl")) hold $(mode == "error" ? ["error"] : String[])`
    p = run(pipeline(cmd; stdout = io, stderr = io); wait = false)
    t0 = time()
    while !occursin("READY", read(log, String))
        process_exited(p) && error("hold exited before READY:\n" * read(log, String))
        time() - t0 > 600 && (kill(p); error("no READY after 600 s"))
        sleep(0.2)
    end
    pids = parse.(Int, split(match(r"TREE=([\d,]+)", read(log, String))[1], ','))
    before = living(pids)
    mode == "kill9" && kill(p, Base.SIGKILL)
    wait(p)
    close(io)
    sleep(1)
    a1 = living(pids)
    sleep(4)
    a5 = living(pids)
    println("$mode: browser tree before=$before after 1 s=$a1 after 5 s=$a5 (julia exit code $(p.exitcode), signal $(p.termsignal))")
    foreach(zap, pids)
end
