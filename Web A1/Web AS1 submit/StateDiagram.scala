object StateDiagram {
	def main(args: Array[String]){
		val states = List("carzy","blue sky","jump","free fall","parachute","alive","dead","cloudy")
		var myState = ""
		println("I have a friend")
		println("Sometimes she is " + states(0))
		print("When it is ")

		var r = scala.util.Random
		var chance = r.nextInt(100)

		if (chance >= 50){
			myState = states(7)
			println(myState)
			println("She is " + states(5))
		}

		else{

			myState = states(1)
			println(myState)
			println("She " + states(2))
			var chance1 = r.nextInt(100)
			var r1 = scala.util.Random
			

			if(chance1 < 30){

				println("She uses " + states(4))
				println("She is " + states(5))

			}

			else{

				println("She " + states(3))
				var r2 = scala.util.Random
				var chance2 = r.nextInt(100)
				if(chance2 < 80 ){
					println("She uses " + states(4))
					println("She is " + states(5))

				}
				
				else{
				println("She is " + states(6))
				}
				
			}
		
		}

	}
	
}
