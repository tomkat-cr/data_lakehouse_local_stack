
#!/bin/bash
set -e
# set -o allexport ; . .env ; set +o allexport ;

create_venv() {
	if [ ! -d venv ]; then python -m venv venv; fi
	if [ -d venv ]; then . venv/bin/activate; fi
}

set_envars() {
    export LDFLAGS="-L/usr/local/opt/openssl/lib"
    export CPPFLAGS="-I/usr/local/opt/openssl/include"
}

install() {
    create_venv
    set_envars
	if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
	if [ ! -f requirements.txt ]; then pip install --upgrade pip; pip install pandas matplotlib python-dotenv ; pip freeze > requirements.txt; fi
}

enter() {
    echo "To enter the virtual environment, run:"
    echo "docker exec -ti raygun_analysis bash"
}

logs() {
    echo "To get the logs, run:"
    echo "docker logs -f raygun_analysis"
}

uninstall() {
    if ! deactivate
    then
        echo "Could not deactivate virtual environment"
        echo "continue with the uninstall anyway"
    fi
    rm -rf venv
}

requirements() {
    install
}

run() {
    install
	python raygun_data_analsys.py
    # uninstall
}

docker() {
    docker-compose up -d
    docker ps
}

down() {
    docker-compose down
}

ACTION=$1

# case $ACTION in
#     "install")
#         install
#         ;;
#     "uninstall")
#         uninstall
#         ;;
#     "docker")
#         docker
#         ;;
#     "down")
#         down
#         ;;
#     "enter")
#         enter
#         ;;
#     "logs")
#         logs
#         ;;
#     "requirements")
#         requirements
#         ;;
#     "run")
#         run
#         ;;
#     *)
#         echo "Usage: run_app.sh <install|uninstall|requirements|run>"
#         exit 1
#         ;;
# esac

if [ "$ACTION" = "install" ]; then
    install
fi
if [ "$ACTION" = "uninstall" ]; then
    uninstall
fi
if [ "$ACTION" = "docker" ]; then
    docker
fi
if [ "$ACTION" = "down" ]; then
    down
fi
if [ "$ACTION" = "enter" ]; then
    enter
fi
if [ "$ACTION" = "logs" ]; then
    logs
fi
if [ "$ACTION" = "requirements" ]; then
    requirements
fi
if [ "$ACTION" = "run" ]; then
    run
fi
if [ "$ACTION" = "" ]; then
    echo "Usage: run_app.sh <install|uninstall|requirements|run>"
    exit 1
fi
