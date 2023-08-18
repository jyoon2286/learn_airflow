FRUIT=$1
if [ $FRUIT == APPLE ];then
	echo "You pick Apple!"
elif [ $FRUIT == ORANGE ];then
	echo "You pick Orange!"
elif [ $FRUIT == GRAPE ];then
	echo "You pick Grape!"
else
	echo "You pick other fruits!"
fi
