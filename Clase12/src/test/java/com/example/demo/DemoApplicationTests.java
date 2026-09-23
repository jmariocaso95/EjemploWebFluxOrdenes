package com.example.demo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;


class DemoApplicationTests {

	@Test
	void fn() {

		var list = new java.util.ArrayList<Integer>();
		list.add(1);
		list.add(2);
		list.add(3);

		var result = list.stream()
				.reduce(Integer::sum)
				.orElse(0);
		System.out.println("result = " + result);

		list.stream().filter(a -> a > 1)
				.forEach(integer -> System.out.println("integer = " + integer));

		var text = "Hello World";

		Optional.of(text)
				.map(String::toUpperCase)
				.stream()
				.forEach(System.out::println)
				;


	}

	@Test
	void rct() {

		Mono.just(new MyClass())
//				.filter(myClass -> false)
//				.switchIfEmpty(Mono.error(new RuntimeException("No data found")))
				.map(o -> {
					if(true){
						throw new RuntimeException("No data found");
					}
					return o;
				})
				.doOnError(e -> System.out.println("ERROR: " + e.getMessage()))
				.onErrorResume(e -> {
					System.out.println("Handling error: " + e.getMessage());
					var myClass = new MyClass();
					myClass.setName("Default Name");
					return Mono.just(myClass);
				})
				.filter(myClass -> myClass.getName() != null)
				.map(myClass -> {
					myClass.setName("Pedro");
					return myClass;
				})
				.flatMap(myClass -> {
					var apellido = " Perez";
					return myClass.getName()
							.map(name -> {
								myClass.setName(name + apellido);
								return myClass;
							});
				})
				.doOnSubscribe(s -> System.out.println("Subscribed"))
				.doOnSuccess(myClass -> System.out.println("Success: " + myClass))
				.doOnError(e -> System.out.println("Error: " + e.getMessage()))
				.doFinally((e) -> System.out.println("Finally"))
				.subscribe(System.out::println);


	}

	private static Function<MyClass, Mono<? extends MyClass>> getNoDataFound() {
		return o -> {
			if (true) {
				return Mono.error(new RuntimeException("No data found"));
			}
			return Mono.just(o);
		};
	}


	public static class MyClass {
		private String name;
		private Integer cedula;

		public Integer getCedula() {
			return cedula;
		}

		public void setCedula(Integer cedula) {
			this.cedula = cedula;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Mono<String> getName() {
			return Mono.justOrEmpty(name);
		}

		@Override
		public String toString() {
			return name + " ";
		}
	}



}
