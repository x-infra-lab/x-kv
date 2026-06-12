# Bash completion for xkv-ctl
# Usage: eval "$(java -jar x-kv-ctl.jar --completion)"

_xkv_ctl() {
    local cur prev
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    # Find the command group (skip --pd <arg>).
    local group=""
    local i=1
    while [ $i -lt $COMP_CWORD ]; do
        case "${COMP_WORDS[$i]}" in
            --pd) ((i++)) ;;  # skip --pd and its argument
            --*) ;;
            *)
                if [ -z "$group" ]; then
                    group="${COMP_WORDS[$i]}"
                fi
                ;;
        esac
        ((i++))
    done

    case "$group" in
        "")
            COMPREPLY=($(compgen -W "cluster store region gc --pd --completion" -- "$cur"))
            ;;
        cluster)
            COMPREPLY=($(compgen -W "health id members" -- "$cur"))
            ;;
        store)
            COMPREPLY=($(compgen -W "list info" -- "$cur"))
            ;;
        region)
            COMPREPLY=($(compgen -W "list info --limit" -- "$cur"))
            ;;
        gc)
            COMPREPLY=($(compgen -W "safepoint" -- "$cur"))
            ;;
    esac
}

complete -F _xkv_ctl xkv-ctl
complete -F _xkv_ctl java
