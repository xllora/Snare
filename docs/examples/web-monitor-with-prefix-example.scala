import snare.ui.WebMonitor
import snare.Snare

// Create the snares
val snares = (1 to 9).toList.map((i)=>Snare("X"+i,"my_pool",(o)=>{println(o);true}))

// Start the monitors
snares.map(_.activity=true)

// Start the webui
WebMonitor("/services","my_pool") on 8080
