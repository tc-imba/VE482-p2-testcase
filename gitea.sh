if [ ! -d "tmp" ]; then
  mkdir tmp
fi

if [ ! -d "reports" ]; then
  mkdir reports
fi

# first upload the public key of your grading PC to your git server

pip3 install -r requirements.txt

python3 test.py --generate-answer --times=1

cd tmp
    for i in {1..9}
    do
        git clone "ssh://gitea@focs.ji.sjtu.edu.cn:2222/ve482/pgroup-0${i}.git"
        cd ..
          python3 test.py -p "tmp/pgroup-0${i}" --times=10
          python3 report.py -p "tmp/pgroup-0${i}" -t ${i}
          cp "tmp/pgroup-0${i}/team${i}_report.pdf" "reports/team${i}_report.pdf"
        cd tmp
    done
    for i in {10..12}
    do
        git clone "ssh://gitea@focs.ji.sjtu.edu.cn:2222/ve482/pgroup-${i}.git"
        cd ..
          python3 test.py -p "tmp/pgroup-${i}" --times=10
          python3 report.py -p "tmp/pgroup-${i}" -t ${i}
          cp "tmp/pgroup-${i}/team${i}_report.pdf" "reports/team${i}_report.pdf"
        cd tmp
    done
cd ../

# for i in {1..9}
# do
#   python3 report.py -p "tmp/pgroup-0${i}" -t ${i}
#   cp "tmp/pgroup-0${i}/team${i}_report.pdf" "reports/team${i}_report.pdf"
# done
# for i in {10..12}
# do
#   python3 report.py -p "tmp/pgroup-${i}" -t ${i}
#   cp "tmp/pgroup-${i}/team${i}_report.pdf" "reports/team${i}_report.pdf"
# done
