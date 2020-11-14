awk '{ sub("\r$", ""); print }' script.bash > fake.bash
mv fake.bash script.bash
bash script.bash $1 $2 $3
