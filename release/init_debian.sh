set -x
set -e

USE_MIRRIOR_SOURCE=0

# 软件源
perl -pi -e 's/^\s*?deb/# deb/ig' /etc/apt/sources.list
if ((${USE_MIRRIOR_SOURCE})) ; then
    printf '%s\n' \
    '# 如有需要可自行注释源码镜像以提高 apt update 速度' \
    '# 阿里云镜像源' \
    'deb http://mirrors.aliyun.com/debian/ bullseye main contrib non-free' \
    'deb http://mirrors.aliyun.com/debian/ bullseye-updates main contrib non-free' \
    'deb http://mirrors.aliyun.com/debian/ bullseye-backports main contrib non-free' \
    'deb http://mirrors.aliyun.com/debian-security bullseye-security main contrib non-free' \
    'deb-src http://mirrors.aliyun.com/debian/ bullseye main contrib non-free' \
    'deb-src http://mirrors.aliyun.com/debian/ bullseye-updates main contrib non-free' \
    'deb-src http://mirrors.aliyun.com/debian/ bullseye-backports main contrib non-free' \
    'deb-src http://mirrors.aliyun.com/debian-security bullseye-security main contrib non-free' \
    >> /etc/apt/sources.list
else
    printf '%s\n' \
    '# 官方源' \
    'deb http://deb.debian.org/debian/ bullseye main contrib non-free' \
    'deb http://deb.debian.org/debian/ bullseye-updates contrib main non-free' \
    'deb http://security.debian.org/ bullseye-security contrib main non-free' \
    'deb-src http://deb.debian.org/debian/ bullseye main contrib non-free' \
    >> /etc/apt/sources.list
fi
apt update
apt upgrade -y

# 时区, locale
ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
if ((0)) ; then
    apt install -y locales locales-all
    locale-gen zh_CN.UTF-8
    export LC_ALL=zh_CN.UTF-8
fi

# 基本工具
apt install -y bash-completion less file zip unzip vim bc git git-lfs
perl -pi -e 's/set mouse=a/set mouse-=a/i' /usr/share/vim/vim*/defaults.vim

# 实用工具
if ((1)); then
    apt install -y procps linux-perf lsof netcat net-tools iftop sysstat tcpdump iproute2 openssl curl wget
    # python3
    apt install -y python3 python3-dev python3-pip python3-cryptography python3-setuptools python3-wheel black
    mkdir -p ~/.pip
    if ((${USE_MIRRIOR_SOURCE})) ; then
        printf '%s' \
        '[global]' \
        'index-url=https://mirrors.aliyun.com/pypi/simple/' \
        > ~/.pip/pip.conf
    fi
fi

# C++开发工具
if ((1)); then
    apt install -y autoconf automake make libtool pkg-config ninja-build diffutils patchelf gcc g++ gdb nasm gfortran  libgomp1
    apt install -y cmake cmake-curses-gui cmake-format 
fi

# java
if ((1)); then
    apt install -y openjdk-11-jdk
fi

# ssh
if ((1)); then
    apt install -y openssh-server openssh-sftp-server
    perl -pi -e 's/#(PermitRootLogin).*/$1 yes/g' /etc/ssh/sshd_config
    /etc/init.d/ssh start #启动一次，自动准备好运行环境
    # 运行命令:  /usr/sbin/sshd -D -p 9999
fi

# ---- 运行时库 --------------------------------------------------

# ---- 运维配置 --------------------------------------------------------
# 必须放最后，避免apt安装时遇到配置冲突问题

printf '%s\n' \
'fs.file-max=524288' \
'fs.inotify.max_user_watches=524288' \
>> /etc/sysctl.conf
# sysctl -p

perl -pi -e 's/.*(DefaultLimitNOFILE).*/$1=524288/g' /etc/systemd/user.conf
perl -pi -e 's/.*(DefaultLimitNOFILE).*/$1=524288/g' /etc/systemd/system.conf

printf '%s' \
'* soft nofile 524288
* hard nofile 524288

# core dump
# * soft core unlimited
' >> /etc/security/limits.conf


# bash常用设置
printf '%s' \
'#-----------------------------------------------------------------
export LC_ALL=C.UTF-8

# enable color support of ls and also add handy aliases
if [ -x /usr/bin/dircolors ]; then
    test -r ~/.dircolors && eval "$(dircolors -b ~/.dircolors)" || eval "$(dircolors -b)"
    alias ls="ls --color=auto"
    alias dir="dir --color=auto"
    alias vdir="vdir --color=auto"

    alias grep="grep --color=auto"
    alias fgrep="fgrep --color=auto"
    alias egrep="egrep --color=auto"
fi

# some more ls aliases
alias ll="ls -l"
alias la="ls -A"
alias l="ls -CF"

# colored GCC warnings and errors
export GCC_COLORS="error=01;31:warning=01;35:note=01;36:caret=01;32:locus=01:quote=01"

if [ -z "${B1F89D1E_E51F_4BA3_8382_A3749DEE2E4A}" ] ; then
    export B1F89D1E_E51F_4BA3_8382_A3749DEE2E4A=1
    # 只执行一次的命令
    
fi
' | tee -a /etc/bash.bashrc

printf '%s' \
'
if ! shopt -oq posix; then
  if [ -f /usr/share/bash-completion/bash_completion ]; then
    . /usr/share/bash-completion/bash_completion
  elif [ -f /etc/bash_completion ]; then
    . /etc/bash_completion
  fi
fi
' | tee -a /etc/bash.bashrc
